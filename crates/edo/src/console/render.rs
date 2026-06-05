//! Inline ratatui canvas: the user-visible build console.
//!
//! ## Architecture
//!
//! A single render task (spawned via [`spawn_render_task`]) owns the
//! [`ratatui::DefaultTerminal`]. Producers communicate with it through
//! an mpsc channel of [`RenderMsg`]:
//!
//! - `Event` — apply one [`ConsoleEvent`] to [`BuildState`] and
//!   maybe scroll a "completed task" line above the canvas via
//!   `Terminal::insert_before`.
//! - `Prompt` — show the failure prompt overlay; resolve the
//!   user's choice through the supplied `oneshot::Sender`.
//! - `Shutdown` — drain remaining events, restore the terminal, exit.
//!
//! Because the render task holds the terminal exclusively no caller
//! ever needs to "suspend" it: keypress handling, the prompt, and the
//! "view log" overlay all live inside the same loop.
//!
//! ## Inline viewport
//!
//! The canvas uses [`ratatui::Viewport::Inline`] anchored to a fixed
//! N-row region at the bottom of the terminal. Lines above the canvas
//! stay scrollback-intact. Diagnostics and finished tasks scroll *into*
//! that scrollback via `Terminal::insert_before`.
//!
//! All ratatui usage is contained in this file so a future renderer
//! swap is a single-file change.

use std::io;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use ratatui::Viewport;
use ratatui::backend::CrosstermBackend;
use ratatui::buffer::Buffer;
use ratatui::crossterm::event::{
    Event as CtEvent, EventStream, KeyCode, KeyEvent, KeyEventKind, KeyModifiers,
};
use ratatui::crossterm::{cursor, execute, terminal as ct_terminal};
use ratatui::crossterm::terminal::{EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Widget, Wrap};
use ratatui::{Frame, Terminal, TerminalOptions};
use tokio::sync::{mpsc, oneshot};
use futures::StreamExt;

use crate::context::Addr;

use super::event::{ConsoleEvent, Severity};
use super::state::{ActiveTask, BuildState};

/// Choice returned by an interactive failure prompt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptChoice {
    /// User asked to retry the failed transform.
    Retry,
    /// User asked to abort the build.
    Quit,
}

/// Request driving an interactive failure prompt.
pub struct PromptRequest {
    /// Address of the failed transform.
    pub addr: Addr,
    /// Stringified error message.
    pub error: String,
    /// Optional path to the per-task `.log` file.
    pub log_file: Option<PathBuf>,
    /// Whether retry is offered.
    pub allow_retry: bool,
    /// Whether opening a shell is offered.
    pub allow_shell: bool,
    /// Callback invoked when the user picks "shell". The render task
    /// suspends the canvas (drops raw mode), runs the closure
    /// inline, then re-installs the canvas. Boxed so the trait object
    /// is `Send` and can cross the channel.
    pub shell: Option<Box<dyn FnMut() -> io::Result<()> + Send>>,
}

/// Messages accepted by the render task.
pub enum RenderMsg {
    /// One [`ConsoleEvent`] to fold into [`BuildState`].
    Event(ConsoleEvent),
    /// Show a failure prompt; resolve to a [`PromptChoice`].
    Prompt {
        request: PromptRequest,
        response: oneshot::Sender<PromptChoice>,
    },
    /// Drain pending events, restore the terminal, exit the loop.
    Shutdown {
        ack: oneshot::Sender<()>,
    },
}

/// Format an elapsed [`Duration`] as `<seconds>.<tenths>s`.
pub fn elapsed_subsec(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    let sub_seconds = (elapsed.as_millis() % 1000) / 100;
    format!("{seconds}.{sub_seconds}s")
}

/// Spawn the render task.
///
/// Returns the channel the producer side ([`super::CanvasSink`] and
/// [`super::Console::prompt`]) writes into. The returned task handle
/// completes when [`RenderMsg::Shutdown`] is processed.
pub fn spawn_render_task(
    height: u16,
) -> (
    mpsc::UnboundedSender<RenderMsg>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        if let Err(e) = run(rx, height).await {
            // The renderer must never poison the build. Log and exit.
            tracing::warn!(subsystem = "console", "render task exited: {e}");
        }
        // Best-effort restore on any exit path.
        let _ = restore_terminal();
    });
    (tx, handle)
}

/// Tear down whatever the canvas left on screen.
///
/// Emits a trailing newline so a subsequent shell prompt does not start
/// on the same row as the inline viewport's last frame (U1).
///
/// Also clears from the cursor downward as a defensive measure: by the
/// time we get here the render task has already issued
/// `terminal.clear()` to wipe the inline viewport, but if a frame
/// landed afterwards (e.g. a final tick raced shutdown) we erase it.
fn restore_terminal() -> io::Result<()> {
    let _ = ct_terminal::disable_raw_mode();
    let mut out = io::stderr();
    use ratatui::crossterm::terminal::{Clear, ClearType};
    let _ = execute!(out, Clear(ClearType::FromCursorDown), cursor::Show);
    // Inline viewport leaves the cursor at the start of the canvas;
    // emit a newline so the user's shell prompt lands on a fresh row.
    use std::io::Write as _;
    let _ = writeln!(out);
    let _ = out.flush();
    Ok(())
}

/// Initialise a [`Terminal`] with the inline viewport on stderr.
fn init_terminal(height: u16) -> io::Result<Terminal<CrosstermBackend<io::Stderr>>> {
    ct_terminal::enable_raw_mode()?;
    let backend = CrosstermBackend::new(io::stderr());
    Terminal::with_options(
        backend,
        TerminalOptions {
            viewport: Viewport::Inline(height.max(3)),
        },
    )
}

/// Render-task entry point. Single-tasked; owns the terminal.
async fn run(
    mut rx: mpsc::UnboundedReceiver<RenderMsg>,
    requested_height: u16,
) -> io::Result<()> {
    let mut terminal = init_terminal(requested_height)?;
    let mut state = BuildState::new();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            msg = rx.recv() => {
                let Some(msg) = msg else { break; };
                match msg {
                    RenderMsg::Event(ev) => {
                        scroll_completion_above(&mut terminal, &ev);
                        state.apply(&ev);
                        let _ = terminal.draw(|frame| draw_main(frame, &state));
                        if matches!(ev, ConsoleEvent::BuildFinished { .. }) {
                            // Final flush — keep the canvas one tick longer
                            // so the user sees the summary before any caller-
                            // initiated shutdown clears it.
                        }
                    }
                    RenderMsg::Prompt { request, response } => {
                        let choice = handle_prompt(&mut terminal, &state, request, &mut rx).await?;
                        let _ = response.send(choice);
                    }
                    RenderMsg::Shutdown { ack } => {
                        // Scroll a final one-line summary into scrollback,
                        // then clear the inline viewport so leftover rows
                        // can't interleave with whatever the calling
                        // process prints next (e.g. a snafu error chain).
                        // Without this, ratatui's last frame remains on
                        // screen as static text and the user sees the
                        // scheduler's `Error:` line bisecting the
                        // active-task table.
                        scroll_final_summary(&mut terminal, &state);
                        // Empty draw paints all viewport cells with
                        // spaces, overwriting residual content.
                        let _ = terminal.draw(|_frame| {});
                        let _ = terminal.clear();
                        // Drop terminal: ratatui's `Drop` does NOT restore
                        // the terminal automatically — we explicitly disable
                        // raw mode below.
                        drop(terminal);
                        let _ = restore_terminal();
                        let _ = ack.send(());
                        return Ok(());
                    }
                }
            }
            _ = tick.tick() => {
                let _ = terminal.draw(|frame| draw_main(frame, &state));
            }
        }
    }
    let _ = restore_terminal();
    Ok(())
}

/// For events that finish a node, scroll a single completion line into
/// the scrollback above the canvas. Diagnostics also scroll there.
///
/// Pre-build provenance events (`SessionStarted`, `ProjectLoaded`)
/// scroll a multi-line header block so the user (and the JSONL
/// consumer reading the same events post-mortem) can see what they
/// invoked, what got loaded, and which `edo` version produced the
/// result.
fn scroll_completion_above<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    ev: &ConsoleEvent,
) {
    // Multi-line events render their own block via `insert_before` and
    // return early. Single-line events fall through to the bottom.
    match ev {
        ConsoleEvent::SessionStarted {
            edo_version,
            target,
            args,
            started_at_unix: _,
        } => {
            // Format args inline; if the list is empty, omit the row.
            let mut lines: Vec<Line<'static>> = Vec::with_capacity(3);
            lines.push(Line::from(Span::styled(
                format!("edo {edo_version}"),
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(vec![
                Span::styled("  target  ", Style::default().fg(Color::DarkGray)),
                Span::raw(target.clone()),
            ]));
            if !args.is_empty() {
                let body = args
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                lines.push(Line::from(vec![
                    Span::styled("  args    ", Style::default().fg(Color::DarkGray)),
                    Span::raw(body),
                ]));
            }
            insert_block(terminal, lines);
            return;
        }
        ConsoleEvent::ProjectLoaded {
            root,
            transforms,
            sources,
            vendors,
            farms,
            caches,
            locked,
        } => {
            let lock_tag = if *locked { "locked" } else { "resolving" };
            let lines = vec![
                Line::from(vec![
                    Span::styled("  project ", Style::default().fg(Color::DarkGray)),
                    Span::raw(root.clone()),
                ]),
                Line::from(vec![
                    Span::styled("  loaded  ", Style::default().fg(Color::DarkGray)),
                    Span::raw(format!(
                        "{transforms} transforms, {sources} sources, {vendors} vendors, {farms} farms, {caches} caches ({lock_tag})"
                    )),
                ]),
            ];
            insert_block(terminal, lines);
            return;
        }
        _ => {}
    }
    let line = match ev {
        ConsoleEvent::EnvSetupFarmFinished { addr, ok, elapsed_ms } => {
            let secs = (*elapsed_ms as f64) / 1000.0;
            if *ok {
                Some(Line::from(vec![
                    Span::styled("✓ ", Style::default().fg(Color::Green)),
                    Span::styled("env ", Style::default().fg(Color::DarkGray)),
                    Span::raw(format!("{addr}")),
                    Span::styled(format!(" ({secs:.1}s)"), Style::default().fg(Color::DarkGray)),
                ]))
            } else {
                Some(Line::from(vec![
                    Span::styled("✗ ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
                    Span::styled("env ", Style::default().fg(Color::DarkGray)),
                    Span::raw(format!("{addr}")),
                    Span::styled(format!(" ({secs:.1}s)"), Style::default().fg(Color::DarkGray)),
                ]))
            }
        }
        ConsoleEvent::NodeCacheHit { addr, .. } => {
            Some(Line::from(vec![
                Span::styled("✓ ", Style::default().fg(Color::Green)),
                Span::raw(format!("{addr}")),
                Span::styled(" (cache)", Style::default().fg(Color::DarkGray)),
            ]))
        }
        ConsoleEvent::NodeFinished { addr, ok, elapsed_ms } => {
            let secs = (*elapsed_ms as f64) / 1000.0;
            if *ok {
                Some(Line::from(vec![
                    Span::styled("✓ ", Style::default().fg(Color::Green)),
                    Span::raw(format!("{addr}")),
                    Span::styled(format!(" ({secs:.1}s)"), Style::default().fg(Color::DarkGray)),
                ]))
            } else {
                Some(Line::from(vec![
                    Span::styled("✗ ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
                    Span::raw(format!("{addr}")),
                    Span::styled(format!(" ({secs:.1}s)"), Style::default().fg(Color::DarkGray)),
                ]))
            }
        }
        ConsoleEvent::Diagnostic { severity, target, message } => {
            let (sym, color) = match severity {
                Severity::Info => ("ℹ", Color::Blue),
                Severity::Warn => ("⚠", Color::Yellow),
                Severity::Error => ("✗", Color::Red),
            };
            Some(Line::from(vec![
                Span::styled(format!("{sym} "), Style::default().fg(color)),
                Span::styled(format!("[{target}] "), Style::default().fg(Color::DarkGray)),
                Span::raw(message.clone()),
            ]))
        }
        _ => None,
    };
    if let Some(line) = line {
        let _ = terminal.insert_before(1, |buf: &mut Buffer| {
            let area = buf.area;
            let p = Paragraph::new(line).wrap(Wrap { trim: false });
            p.render(area, buf);
        });
    }
}

/// Scroll a multi-line block of `Line`s into scrollback above the
/// inline viewport. The block height is `lines.len()` rows; each line
/// is rendered with wrapping disabled so terminal-narrow projects fold
/// gracefully rather than truncating mid-word.
fn insert_block<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    lines: Vec<Line<'static>>,
) {
    let height = lines.len() as u16;
    if height == 0 {
        return;
    }
    let _ = terminal.insert_before(height, |buf: &mut Buffer| {
        let area = buf.area;
        let p = Paragraph::new(lines).wrap(Wrap { trim: false });
        p.render(area, buf);
    });
}

/// Scroll a single-line build summary into scrollback right before
/// teardown. Mirrors what the canvas header was showing but lives
/// permanently above the (about-to-be-cleared) viewport so the user
/// retains a record of how the build ended.
///
/// Includes the wall-clock build duration from `BuildFinished` and
/// (when known) the edo version captured from `SessionStarted`, so
/// the line in scrollback is self-contained provenance.
fn scroll_final_summary<B: ratatui::backend::Backend>(
    terminal: &mut Terminal<B>,
    state: &BuildState,
) {
    let total = state.total.max(state.finished);
    let root = state
        .root
        .as_ref()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "<unknown>".to_string());
    let (sym, color, head) = if state.done && state.ok {
        ("✓", Color::Green, "BUILD ok")
    } else {
        ("✗", Color::Red, "BUILD failed")
    };
    let elapsed_secs = (state.elapsed_ms as f64) / 1000.0;
    let body = format!(
        " {head}  {done}/{total}  ({hits} cached, {ran} built)  failed {failed}  in {elapsed_secs:.1}s  {root}",
        done = state.finished,
        total = total,
        hits = state.cache_hits,
        ran = state.transforms_finished,
        failed = state.failed.len(),
        elapsed_secs = elapsed_secs,
        root = root,
    );
    let mut spans = vec![
        Span::styled(format!("{sym}"), Style::default().fg(color).add_modifier(Modifier::BOLD)),
        Span::styled(body, Style::default().add_modifier(Modifier::BOLD)),
    ];
    if let Some(v) = state.edo_version.as_ref() {
        spans.push(Span::styled(
            format!("  (edo {v})"),
            Style::default().fg(Color::DarkGray),
        ));
    }
    let line = Line::from(spans);
    let _ = terminal.insert_before(1, |buf: &mut Buffer| {
        let area = buf.area;
        Paragraph::new(line).wrap(Wrap { trim: false }).render(area, buf);
    });
}

/// Compose the canvas: header line + active-task table.
fn draw_main(frame: &mut Frame, state: &BuildState) {
    let area = frame.area();
    let chunks = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(area);
    render_header(frame, chunks[0], state);
    render_active_tasks(frame, chunks[1], state);
}

fn render_header(frame: &mut Frame, area: Rect, state: &BuildState) {
    let total = state.total.max(state.finished);
    let cache_pct = (state.cache_ratio() * 100.0).round() as u32;
    let running = state.active_running();
    let pending = state.transforms_pending();
    let root = state
        .root
        .as_ref()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "<unknown>".to_string());
    // The header reports two distinct progress signals:
    //   1. `done/total` — overall completion (cache hits + real runs).
    //   2. `transforms run/pending` — actual build work, so a
    //      mostly-cached graph doesn't display "100% cache" while
    //      hours of real transforms still run. (Issue #1.)
    let msg = if state.env_setup_total > 0 && !state.env_setup_done && state.total == 0 {
        // Pre-build farm-setup phase. `state.total` is zero until
        // `BuildStarted` arrives, so this branch only fires before the
        // scheduler kicks off.
        let active = if let Some(addr) = state.env_setup_active.first() {
            format!(" · {addr}")
        } else {
            String::new()
        };
        format!(
            "setting up environments {done}/{total}{active}",
            done = state.env_setup_finished,
            total = state.env_setup_total,
            active = active,
        )
    } else if state.done {
        if state.ok {
            format!(
                "BUILD ok  {done}/{total}  ({hits} cached, {ran} built)  {root}",
                done = state.finished,
                total = total,
                hits = state.cache_hits,
                ran = state.transforms_finished,
                root = root,
            )
        } else {
            format!(
                "BUILD failed  {done}/{total}  failed {failed}  ({hits} cached, {ran} built)  {root}",
                done = state.finished,
                total = total,
                failed = state.failed.len(),
                hits = state.cache_hits,
                ran = state.transforms_finished,
                root = root,
            )
        }
    } else if pending > 0 || running > 0 {
        let waiting = state.waiting();
        // `waiting` is the queue depth of fetched-but-not-yet-running
        // nodes \u2014 useful when the run-pool is the bottleneck. Hidden
        // when zero to keep the header compact.
        if waiting > 0 {
            format!(
                "{done}/{total}  build {ran}/{ran_total}  active {running}  waiting {waiting}  cache {cache}%  {root}",
                done = state.finished,
                total = total,
                ran = state.transforms_finished,
                ran_total = state.transforms_finished + pending,
                running = running,
                waiting = waiting,
                cache = cache_pct,
                root = root,
            )
        } else {
            format!(
                "{done}/{total}  build {ran}/{ran_total}  active {running}  cache {cache}%  {root}",
                done = state.finished,
                total = total,
                ran = state.transforms_finished,
                ran_total = state.transforms_finished + pending,
                running = running,
                cache = cache_pct,
                root = root,
            )
        }
    } else {
        // Pre-fetch / nothing-to-do shape.
        format!(
            "{done}/{total}  cache {cache}%  {root}",
            done = state.finished,
            total = total,
            cache = cache_pct,
            root = root,
        )
    };
    let style = if state.done {
        if state.ok {
            Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
        }
    } else {
        Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)
    };
    let p = Paragraph::new(Line::from(Span::styled(msg, style)));
    frame.render_widget(p, area);
}

fn render_active_tasks(frame: &mut Frame, area: Rect, state: &BuildState) {
    if area.height == 0 {
        return;
    }
    let cap = std::cmp::min(10, (area.height as usize).max(1));

    // Filter to tasks that are *actually doing work*. Nodes parked in
    // `Wait` (fetched, queued for the run-pool) and nodes still in
    // the pre-phase queued state aren't useful to display as 10
    // rotating rows — they crowd out the genuinely in-flight
    // transforms. The header surfaces queue depth via `waiting N`.
    //
    // `Fetch` *is* in-flight work (the node currently holds a
    // semaphore permit and is pulling sources), so it stays visible.
    let is_running = |addr: &&Addr| -> bool {
        match state.active.get(*addr).and_then(|t| t.phase) {
            None => false, // Pre-phase queued (no NodePhase yet)
            Some(super::event::Phase::Wait) => false,
            Some(_) => true, // Fetch + every transform-lifecycle phase
        }
    };
    let mut rows: Vec<&Addr> = state.active.keys().filter(is_running).collect();
    // Within running rows, sort by phase priority (most-progressed
    // first) then recency — with a small worker pool the user wants
    // their 8 transforms visible above any teardown stragglers.
    rows.sort_by_key(|a| {
        let task = state.active.get(*a);
        let pri = task.and_then(|t| t.phase).map(|p| p.priority()).unwrap_or(0);
        let started = task.map(|t| t.started);
        (std::cmp::Reverse(pri), std::cmp::Reverse(started))
    });
    let now = Instant::now();
    let visible = rows.iter().take(cap);
    let mut lines: Vec<Line> = Vec::new();
    for addr in visible {
        let task = match state.active.get(*addr) {
            Some(t) => t,
            None => continue,
        };
        lines.push(format_active_row(addr, task, now));
    }
    if rows.len() > cap {
        let extra = rows.len() - cap;
        lines.push(Line::from(Span::styled(
            format!("  (+{extra} more running)"),
            Style::default().fg(Color::DarkGray),
        )));
    }
    let p = Paragraph::new(lines);
    frame.render_widget(p, area);
}

fn format_active_row(addr: &Addr, task: &ActiveTask, now: Instant) -> Line<'static> {
    let elapsed = now.saturating_duration_since(task.started);
    let elapsed_str = elapsed_subsec(elapsed);
    let phase = task
        .phase
        .map(|p| p.tag())
        .unwrap_or("QUEUED");
    let row_style = if elapsed > Duration::from_secs(15 * 60) {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    } else if elapsed > Duration::from_secs(5 * 60) {
        Style::default().fg(Color::Yellow)
    } else {
        Style::default().fg(Color::Blue)
    };
    Line::from(vec![
        Span::styled(format!(" {elapsed_str:>7}  "), Style::default().fg(Color::DarkGray)),
        Span::styled(format!("{phase:<6} "), row_style),
        Span::raw(addr.to_string()),
    ])
}

// ── failure prompt ─────────────────────────────────────────────────────────

/// Driver for the failure prompt. Owns the terminal until the user
/// picks `Retry` or `Quit`; "view log" / "shell" are handled inline.
///
/// `rx` is borrowed so we can observe `RenderMsg::Shutdown` while a
/// prompt overlay is up — without that, `Console::shutdown()` deadlocks
/// behind an active prompt (P1 finding).
async fn handle_prompt(
    terminal: &mut Terminal<CrosstermBackend<io::Stderr>>,
    state: &BuildState,
    request: PromptRequest,
    rx: &mut mpsc::UnboundedReceiver<RenderMsg>,
) -> io::Result<PromptChoice> {
    let PromptRequest {
        addr,
        error,
        log_file,
        allow_retry,
        allow_shell,
        mut shell,
    } = request;
    let mut options: Vec<&'static str> = Vec::new();
    if log_file.is_some() {
        options.push("view log");
    }
    if allow_retry {
        options.push("retry");
    }
    if allow_shell {
        options.push("shell");
    }
    options.push("quit");

    let mut selected: usize = 0;
    let mut events = EventStream::new();

    loop {
        // Force a fresh redraw on every iteration so coming back from
        // `view_log` / `run_shell` shows clean options instead of
        // overdraw garbage (U2.3).
        terminal.clear()?;
        terminal.draw(|frame| {
            draw_prompt(frame, state, &addr, &error, log_file.as_ref(), &options, selected)
        })?;
        tokio::select! {
            biased;
            // Watch for shutdown so a caller-initiated teardown does
            // not deadlock behind an active prompt (P1).
            msg = rx.recv() => {
                match msg {
                    Some(RenderMsg::Shutdown { ack }) => {
                        let _ = ack.send(());
                        return Ok(PromptChoice::Quit);
                    }
                    Some(RenderMsg::Event(_)) => {
                        // Background events arriving mid-prompt are
                        // dropped — the prompt owns the canvas. Other
                        // sinks (JSONL, file log) still record them.
                        continue;
                    }
                    Some(RenderMsg::Prompt { response, .. }) => {
                        // A second prompt arrived while one is active.
                        // Reject it cleanly so the producer doesn't hang.
                        let _ = response.send(PromptChoice::Quit);
                        continue;
                    }
                    None => return Ok(PromptChoice::Quit),
                }
            }
            next = events.next() => match next {
                None => return Ok(PromptChoice::Quit),
                Some(Err(_)) => continue,
                Some(Ok(CtEvent::Key(KeyEvent { kind, .. }))) if kind != KeyEventKind::Press => {
                    continue;
                }
                Some(Ok(CtEvent::Key(KeyEvent {
                    code, modifiers, ..
                }))) => {
                    match code {
                        KeyCode::Up | KeyCode::Char('k') => {
                            selected = selected.saturating_sub(1);
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            selected = (selected + 1).min(options.len() - 1);
                        }
                        KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => {
                            return Ok(PromptChoice::Quit);
                        }
                        KeyCode::Esc => return Ok(PromptChoice::Quit),
                        KeyCode::Char('q') => return Ok(PromptChoice::Quit),
                        KeyCode::Char('r') if allow_retry => return Ok(PromptChoice::Retry),
                        KeyCode::Char('v') if log_file.is_some() => {
                            if let Some(p) = log_file.clone() {
                                view_log(terminal, state, &p, &p).await?;
                            }
                        }
                        KeyCode::Char('s') if allow_shell => {
                            run_shell(terminal, &mut shell).await?;
                        }
                        KeyCode::Enter => match options[selected] {
                            "view log" => {
                                if let Some(p) = log_file.clone() {
                                    view_log(terminal, state, &p, &p).await?;
                                }
                            }
                            "retry" => return Ok(PromptChoice::Retry),
                            "shell" => run_shell(terminal, &mut shell).await?,
                            "quit" => return Ok(PromptChoice::Quit),
                            _ => {}
                        },
                        _ => {}
                    }
                }
                Some(Ok(_)) => {}
            }
        }
    }
}

fn draw_prompt(
    frame: &mut Frame,
    state: &BuildState,
    addr: &Addr,
    error: &str,
    log_file: Option<&PathBuf>,
    options: &[&'static str],
    selected: usize,
) {
    let area = frame.area();
    let chunks = Layout::vertical([
        Constraint::Length(1), // header
        Constraint::Length(2), // failure summary
        Constraint::Min(1),    // option list
    ])
    .split(area);
    render_header(frame, chunks[0], state);

    let mut summary = vec![Line::from(vec![
        Span::styled(
            "✗ ",
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            addr.to_string(),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(": "),
        Span::raw(error.to_string()),
    ])];
    if let Some(p) = log_file {
        summary.push(Line::from(vec![
            Span::styled("  log: ", Style::default().fg(Color::DarkGray)),
            Span::raw(p.display().to_string()),
        ]));
    }
    frame.render_widget(Paragraph::new(summary).wrap(Wrap { trim: false }), chunks[1]);

    let opt_lines: Vec<Line> = options
        .iter()
        .enumerate()
        .map(|(i, opt)| {
            let marker = if i == selected { "▶ " } else { "  " };
            let style = if i == selected {
                Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            Line::from(vec![Span::styled(marker, style), Span::styled(*opt, style)])
        })
        .collect();
    frame.render_widget(Paragraph::new(opt_lines), chunks[2]);
}

/// Display a per-task log file inside an overlay. Esc / q closes.
///
/// Renders full-screen via the alternate-screen buffer so the user
/// gets the entire terminal height to read the log (U2.1) instead of
/// the inline viewport's 8 rows. On exit the alternate screen is
/// dropped and the inline viewport is reanchored by clearing the
/// caller's terminal — `handle_prompt`'s next iteration also issues
/// a `terminal.clear()` for belt-and-braces (U2.3).
async fn view_log(
    terminal: &mut Terminal<CrosstermBackend<io::Stderr>>,
    state: &BuildState,
    log_path_label: &std::path::Path,
    path: &std::path::Path,
) -> io::Result<()> {
    // Read off the runtime — the file may be large enough to stall the
    // render task if read inline.
    let contents = tokio::task::spawn_blocking({
        let path = path.to_path_buf();
        move || std::fs::read_to_string(&path)
    })
    .await
    .map_err(|e| io::Error::other(format!("log read join error: {e}")))?
    .unwrap_or_else(|e| format!("<failed to read log: {e}>"));
    let lines: Vec<&str> = contents.lines().collect();
    let mut offset: usize = lines.len().saturating_sub(50); // tail by default
    let mut events = EventStream::new();

    // Switch to the alternate screen and build a fullscreen ratatui
    // terminal for the duration of the overlay. Restored on every exit
    // path below.
    let _ = execute!(io::stderr(), EnterAlternateScreen);
    let backend = CrosstermBackend::new(io::stderr());
    let mut full = match Terminal::new(backend) {
        Ok(t) => t,
        Err(e) => {
            let _ = execute!(io::stderr(), LeaveAlternateScreen);
            return Err(e);
        }
    };
    let result = view_log_loop(
        &mut full,
        state,
        log_path_label,
        &lines,
        &mut offset,
        &mut events,
    )
    .await;
    drop(full);
    let _ = execute!(io::stderr(), LeaveAlternateScreen);

    // Force the caller's inline viewport to reanchor on the next draw
    // (U2.3) — without this the prompt redraws on top of stale rows.
    let _ = terminal.clear();
    result
}

/// Inner key-event loop for [`view_log`], factored out so the
/// surrounding alternate-screen scope can `?`-propagate cleanly.
async fn view_log_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stderr>>,
    state: &BuildState,
    log_path_label: &std::path::Path,
    lines: &[&str],
    offset: &mut usize,
    events: &mut EventStream,
) -> io::Result<()> {
    loop {
        terminal.draw(|frame| draw_log_view(frame, state, log_path_label, lines, *offset))?;
        match events.next().await {
            None => return Ok(()),
            Some(Err(_)) => continue,
            Some(Ok(CtEvent::Key(KeyEvent { kind, .. }))) if kind != KeyEventKind::Press => continue,
            Some(Ok(CtEvent::Key(KeyEvent { code, modifiers, .. }))) => match code {
                KeyCode::Esc | KeyCode::Char('q') => return Ok(()),
                KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => return Ok(()),
                KeyCode::Up | KeyCode::Char('k') => {
                    *offset = offset.saturating_sub(1);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    *offset = (*offset + 1).min(lines.len().saturating_sub(1));
                }
                KeyCode::PageUp => {
                    *offset = offset.saturating_sub(10);
                }
                KeyCode::PageDown => {
                    *offset = (*offset + 10).min(lines.len().saturating_sub(1));
                }
                KeyCode::Home => *offset = 0,
                KeyCode::End => *offset = lines.len().saturating_sub(1),
                _ => {}
            },
            Some(Ok(_)) => {}
        }
    }
}

fn draw_log_view(
    frame: &mut Frame,
    state: &BuildState,
    log_path_label: &std::path::Path,
    lines: &[&str],
    offset: usize,
) {
    let area = frame.area();
    let chunks = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(area);
    render_header(frame, chunks[0], state);

    let title = format!(
        "log: {}  ({} lines, ↑↓ scroll, q close)",
        log_path_label.display(),
        lines.len()
    );
    let block = Block::default()
        .title(title)
        .borders(Borders::TOP)
        .border_style(Style::default().fg(Color::DarkGray));
    let inner = block.inner(chunks[1]);
    frame.render_widget(block, chunks[1]);
    let visible = (inner.height as usize).max(1);
    let end = (offset + visible).min(lines.len());
    let body: Vec<Line> = lines[offset..end]
        .iter()
        .map(|l| Line::from(Span::raw((*l).to_string())))
        .collect();
    frame.render_widget(Paragraph::new(body), inner);
}

/// Suspend the canvas, run the user-supplied shell callback, restore.
///
/// The callback is held as `&mut Option<Box<FnMut..>>` and re-installed
/// after each invocation rather than `take()`d permanently (P1) — so a
/// user can drop into the shell, exit, then choose `shell` again from
/// the same prompt.
///
/// The inline viewport is fully torn down (raw mode off, alternate
/// screen left, trailing newline) before the child process runs so the
/// shell sees a clean tty (U2.2) instead of overlaying the prompt rows.
/// On return the caller's terminal is cleared so the prompt redraws
/// from scratch (U2.3).
async fn run_shell(
    terminal: &mut Terminal<CrosstermBackend<io::Stderr>>,
    shell: &mut Option<Box<dyn FnMut() -> io::Result<()> + Send>>,
) -> io::Result<()> {
    if shell.is_none() {
        return Ok(());
    }
    // Tear the inline viewport down completely.
    let _ = ct_terminal::disable_raw_mode();
    let _ = execute!(
        io::stderr(),
        LeaveAlternateScreen,
        cursor::Show,
    );
    {
        use std::io::Write as _;
        let mut out = io::stderr();
        let _ = writeln!(out);
        let _ = out.flush();
    }

    // Move the FnMut out of the Option just for the duration of the
    // call so we can hand ownership to spawn_blocking, then put it
    // back on return — preserving repeat-invocation.
    let mut cb = shell.take().expect("checked is_some above");
    let join = tokio::task::spawn_blocking(move || {
        let r = cb();
        (cb, r)
    })
    .await;

    // Restore the inline viewport: re-enable raw mode and clear.
    let _ = ct_terminal::enable_raw_mode();
    let _ = terminal.clear();

    match join {
        Ok((cb_back, Ok(()))) => {
            *shell = Some(cb_back);
            Ok(())
        }
        Ok((cb_back, Err(e))) => {
            *shell = Some(cb_back);
            Err(e)
        }
        Err(join_err) => Err(io::Error::other(format!("shell join error: {join_err}"))),
    }
}

// ── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Id;

    #[test]
    fn elapsed_subsec_formats_tenths() {
        assert_eq!(elapsed_subsec(Duration::from_millis(0)), "0.0s");
        assert_eq!(elapsed_subsec(Duration::from_millis(123)), "0.1s");
        assert_eq!(elapsed_subsec(Duration::from_millis(1700)), "1.7s");
        assert_eq!(elapsed_subsec(Duration::from_secs(42)), "42.0s");
    }

    fn id_for(name: &str) -> Id {
        Id::builder()
            .name(name.to_string())
            .digest("d".to_string())
            .build()
    }

    #[test]
    fn format_active_row_renders_addr_and_phase_tag() {
        let mut state = BuildState::new();
        let a = Addr::parse("//a/b").unwrap();
        state.apply(&ConsoleEvent::NodeQueued {
            addr: a.clone(),
            id: Some(id_for("a")),
        });
        state.apply(&ConsoleEvent::NodePhase {
            addr: a.clone(),
            phase: super::super::event::Phase::Execute,
        });
        let task = state.active.get(&a).unwrap();
        let line = format_active_row(&a, task, Instant::now());
        // Spans contain the addr string and the phase tag somewhere.
        let rendered: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
        assert!(rendered.contains("//a/b"), "got {rendered}");
        assert!(rendered.contains("EXEC"), "got {rendered}");
    }
}

