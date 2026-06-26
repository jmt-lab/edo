use crate::{
    context::Addr,
    tui::{
        event::{Event, Task, TaskStatus},
        state::State,
    },
};
use crossterm::{
    cursor, execute,
    terminal::{Clear, ClearType, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal, TerminalOptions, Viewport,
    backend::CrosstermBackend,
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Paragraph, Widget, Wrap},
};
use tokio::sync::mpsc::UnboundedReceiver;

use std::{
    io::{self, Write},
    time::Duration,
};

pub struct UI {
    receiver: UnboundedReceiver<Event>,
    terminal: Terminal<CrosstermBackend<io::Stderr>>,
    state: State,
}

impl UI {
    pub fn init(rx: UnboundedReceiver<Event>, height: u16) -> io::Result<Self> {
        enable_raw_mode()?;
        Ok(Self {
            receiver: rx,
            terminal: Terminal::with_options(
                CrosstermBackend::new(io::stderr()),
                TerminalOptions {
                    viewport: Viewport::Inline(height.max(3)),
                },
            )?,
            state: State::default(),
        })
    }

    pub async fn run(&mut self) -> io::Result<()> {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                biased;
                msg = self.receiver.recv() => {
                    if let Some(event) = msg.as_ref() {
                        if *event == Event::Terminate {
                            break;
                        }
                        self.state.apply(event);
                        let lines = event.to_lines();
                        if !lines.is_empty() {
                            let height = lines.len() as u16;
                            self.terminal.insert_before(height, |buf: &mut Buffer| {
                                let area = buf.area;
                                let p = Paragraph::new(lines).wrap(Wrap { trim: false });
                                p.render(area, buf);
                            })?;
                        }

                    }
                    self.terminal.draw(|frame| Self::draw(&self.state, frame))?;
                }
                _ = tick.tick() => {
                    self.terminal.draw(|frame| Self::draw(&self.state, frame))?;
                }
            }
        }
        self.restore_terminal()?;
        Ok(())
    }

    fn restore_terminal(&self) -> io::Result<()> {
        disable_raw_mode()?;
        let mut out = io::stderr();
        execute!(out, Clear(ClearType::FromCursorDown), cursor::Show)?;
        writeln!(out)?;
        out.flush()?;
        Ok(())
    }

    fn draw(state: &State, frame: &mut Frame) {
        let area = frame.area();
        let chunks = Layout::vertical([Constraint::Length(1), Constraint::Min(1)]).split(area);
        Self::render_tasks(state, frame, chunks[1]);
        Self::render_statusline(state, frame, chunks[0]);
    }

    fn render_statusline(state: &State, frame: &mut Frame, area: Rect) {
        let total = state.total.max(state.finished);
        let root = state
            .addr
            .as_ref()
            .map(|x| x.to_string())
            .unwrap_or_default();
        let msg = if state.done {
            if state.ok {
                format!(
                    "ok {done}/{total} {root}",
                    done = state.finished,
                    total = total,
                    root = root
                )
            } else {
                format!(
                    "failed {done}/{total} failed {failed} {root}",
                    done = state.finished,
                    total = total,
                    failed = state.failed.len(),
                    root = root
                )
            }
        } else if state.waiting > 0 || state.in_flight > 0 {
            if state.waiting > 0 {
                format!(
                    "{done}/{total} active {running} waiting {waiting} {root}",
                    done = state.finished,
                    total = total,
                    running = state.in_flight,
                    waiting = state.waiting,
                    root = root
                )
            } else {
                format!(
                    "{done}/{total} active {running} {root}",
                    done = state.finished,
                    total = total,
                    running = state.in_flight,
                    root = root
                )
            }
        } else {
            format!(
                "{done}/{total} {root}",
                done = state.finished,
                total = total,
                root = root
            )
        };
        let style = if state.done {
            if state.ok {
                Style::default().fg(Color::Green).bold()
            } else {
                Style::default().fg(Color::Red).bold()
            }
        } else {
            Style::default().fg(Color::Cyan).bold()
        };
        let p = Paragraph::new(Line::from(Span::styled(msg, style)));
        frame.render_widget(p, area);
    }

    fn render_tasks(state: &State, frame: &mut Frame, area: Rect) {
        if area.height == 0 {
            return;
        }
        let cap = std::cmp::min(10, (area.height as usize).max(1));

        let is_running = |addr: &&String| -> bool {
            match state.active.get(*addr).map(|t| &t.status) {
                None => false,
                Some(TaskStatus::Running) => true,
                Some(_) => false,
            }
        };
        let mut rows: Vec<&String> = state.active.keys().filter(is_running).collect();
        rows.sort_by_key(|a| {
            let task = state.active.get(*a);
            let started = task.map(|t| t.status_since);
            std::cmp::Reverse(started)
        });
        let visible = rows.iter().take(cap);
        let mut lines: Vec<Line> = Vec::new();
        for addr in visible {
            let task = match state.active.get(*addr) {
                Some(t) => t,
                None => continue,
            };
            lines.push(task.to_line());
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
}
