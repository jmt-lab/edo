use std::sync::{Arc, OnceLock};

use crate::{
    context::Addr,
    tui::{
        event::{Event, Severity, TaskStatus},
        ui::UI,
    },
};
use jiff::Timestamp;
use parking_lot::Mutex;

pub mod event;
mod state;
mod ui;

pub use event::{Event as TuiEvent, Severity as TuiSeverity, TaskStatus as TuiTaskStatus};

/// Global console handle.
///
/// Initialized exactly once by [`Console::install`] (typically from
/// [`crate::context::Context::init`]). Macros like [`ui_info!`] and the
/// scheduler / context emit helpers all reach for it via `CONSOLE.get()`.
pub static CONSOLE: OnceLock<Console> = OnceLock::new();

/// Choice returned by an interactive failure prompt.
///
/// Stubbed for now — the new tui does not yet implement an interactive
/// prompt, so [`Console::prompt`] always returns [`PromptChoice::Quit`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptChoice {
    /// User asked to retry the failed transform (not currently reachable).
    Retry,
    /// User asked to abort the build (default for the stub).
    Quit,
}

/// Request driving an interactive failure prompt.
///
/// Sent to the UI task via the build-event channel; for now the UI just
/// drops it and the scheduler proceeds as if the user picked
/// [`PromptChoice::Quit`].
pub struct PromptRequest {
    /// Address of the failed transform.
    pub addr: Addr,
    /// Stringified error message.
    pub error: String,
    /// Optional path to the per-task `.log` file.
    pub log_file: Option<std::path::PathBuf>,
    /// Whether retry is offered.
    pub allow_retry: bool,
    /// Whether the user can drop into a shell inside the failed env.
    pub allow_shell: bool,
    /// Shell callback. Unused by the stub.
    pub shell: Option<Box<dyn FnMut() -> std::io::Result<()> + Send>>,
}

/// Cheap-to-clone handle to the build-event console.
pub struct Console {
    handle: Arc<Inner>,
}

impl Console {
    /// Spawn the inline UI task and return a console handle that
    /// forwards events to it.
    pub fn new() -> Self {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let join = tokio::spawn(async move {
            match UI::init(receiver, 8) {
                Ok(mut ui) => {
                    if let Err(e) = ui.run().await {
                        tracing::warn!(subsystem = "tui", "ui exited with error: {e}");
                    }
                }
                Err(e) => {
                    tracing::warn!(subsystem = "tui", "ui failed to init: {e}");
                }
            }
        });
        Self {
            handle: Arc::new(Inner {
                sender,
                join: Mutex::new(Some(join)),
            }),
        }
    }

    /// Install this console as the process-wide [`CONSOLE`]. Idempotent
    /// — subsequent calls drop the new instance and keep the first one.
    pub fn install(self) {
        let _ = CONSOLE.set(self);
    }

    /// Returns the process-wide console handle if one has been installed.
    pub fn global() -> Option<&'static Console> {
        CONSOLE.get()
    }

    pub async fn emit_header(
        &self,
        version: &semver::Version,
        addr: Option<Addr>,
        args: Vec<(String, String)>,
    ) {
        self.send(&Event::Header {
            version: version.clone(),
            addr,
            args,
            started_at: Timestamp::now(),
        })
        .await;
    }

    pub async fn emit_summary<P: AsRef<std::path::Path>>(
        &self,
        path: P,
        transforms: usize,
        sources: usize,
        farms: usize,
        locked: bool,
    ) {
        self.send(&Event::Summary {
            root: path.as_ref().to_path_buf(),
            transforms,
            sources,
            farms,
            locked,
        })
        .await;
    }

    pub async fn start_build(&self, addr: &Addr, total: usize) {
        self.send(&Event::StartBuild {
            addr: addr.clone(),
            total,
        })
        .await;
    }

    pub async fn start_task(
        &self,
        component: &str,
        id: &str,
        operation: &str,
        status: TaskStatus,
        message: Option<String>,
    ) {
        self.send(&Event::StartTask {
            component: component.to_string(),
            id: id.to_string(),
            status,
            operation: operation.to_string(),
            message,
        })
        .await;
    }

    pub async fn update_task(
        &self,
        component: &str,
        id: &str,
        operation: &str,
        status: TaskStatus,
        message: Option<String>,
    ) {
        self.send(&Event::UpdateTask {
            component: component.to_string(),
            id: id.to_string(),
            operation: operation.to_string(),
            status,
            message,
        })
        .await;
    }

    pub async fn emit_diagnostic(
        &self,
        component: &str,
        id: Option<String>,
        severity: Severity,
        message: &str,
    ) {
        self.send(&Event::Diagnostic {
            component: component.to_string(),
            id,
            severity,
            message: message.to_string(),
        })
        .await;
    }

    pub async fn finish_build(&self) {
        self.send(&Event::BuildFinish).await;
    }

    pub async fn emit_terminate(&self) {
        self.send(&Event::Terminate).await;
    }

    pub async fn send(&self, event: &Event) {
        let _ = self.handle.sender.send(event.clone());
    }

    /// Drive an interactive failure prompt.
    ///
    /// **Stub:** the new tui module does not yet implement an
    /// interactive prompt. We accept the request so callers (scheduler
    /// `execute`) can keep flowing through their existing code path,
    /// but we always immediately respond with [`PromptChoice::Quit`]
    /// so the build aborts cleanly on the first failure.
    pub async fn prompt(&self, request: PromptRequest) -> PromptChoice {
        // Drop the shell callback to release any captured handles.
        drop(request.shell);
        // Surface the failure as a diagnostic so the user still sees
        // *what* failed in the UI before we give up on the build.
        let msg = if let Some(path) = request.log_file {
            format!(
                "transform {} failed: {} (log: {})",
                request.addr,
                request.error,
                path.display()
            )
        } else {
            format!("transform {} failed: {}", request.addr, request.error)
        };
        self.emit_diagnostic(
            "scheduler",
            Some(request.addr.to_string()),
            Severity::Error,
            &msg,
        )
        .await;
        let _ = request.allow_retry;
        let _ = request.allow_shell;
        PromptChoice::Quit
    }

    /// Drain the UI task and restore the terminal.
    ///
    /// Idempotent: the JoinHandle is taken out of the inner state on
    /// the first call. The Terminate event flushes any pending draws
    /// and unwinds the ratatui viewport before the join completes.
    pub async fn shutdown(&self) {
        self.emit_terminate().await;
        let join = { self.handle.join.lock().take() };
        if let Some(j) = join {
            let _ = j.await;
        }
    }
}

impl Default for Console {
    fn default() -> Self {
        Self::new()
    }
}

struct Inner {
    sender: tokio::sync::mpsc::UnboundedSender<Event>,
    /// Join handle for the UI task. Taken out by [`Console::shutdown`].
    join: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

/// Shorthand: best-effort send to the global console.
///
/// Used by the `ui_*` macros and the scheduler/context migration helpers.
/// If the console has not been installed (tests, ad-hoc tooling) the call
/// is silently dropped.
#[doc(hidden)]
pub async fn try_send(event: &Event) {
    if let Some(c) = CONSOLE.get() {
        c.send(event).await;
    }
}

#[macro_export]
macro_rules! header {
    ($version: expr) => {
        tracing::info!(version = $version.to_string(), "starting execution");
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_header($version, None, Vec::default()).await;
        }
    };
    ($version: expr => $addr: expr) => {
        tracing::info!(
            version = $version.to_string(),
            target = $addr,
            "starting execution"
        );
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_header($version, Some($addr), Vec::default()).await;
        }
    };
    ($version: expr => $addr: expr, $args: expr) => {
        tracing::info!(
            version = $version.to_string(),
            target = $addr,
            "starting execution"
        );
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_header($version, Some($addr), $args).await;
        }
    };
    ($version: expr, $args: expr) => {
        tracing::info!(version = $version.to_string(), "starting execution");
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_header($version, None, $args).await;
        }
    };
}

#[macro_export]
macro_rules! summary {
    (path = $path: expr, transforms = $t: expr, sources = $s: expr, farms = $f: expr, locked = $l: expr) => {
        tracing::info!(
            path = $path,
            transforms = $t,
            sources = $s,
            farms = $f,
            locked = $l,
            "project loaded"
        );
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_summary($path, $t, $s, $f, $l).await;
        }
    };
}

#[macro_export]
macro_rules! ui_start_build {
    ($addr: expr, $count: expr) => {
        tracing::info!(addr = $addr, total = $count, "starting build");
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.start_build($addr, $count).await;
        }
    };
}

#[macro_export]
macro_rules! ui_trace {
    (component = $component: expr, id = $id: expr, $($arg:tt)*) => {{
        tracing::trace!(component = $component, id = %$id, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                Some($id.to_string()),
                $crate::tui::event::Severity::Trace,
                &format!($($arg)*)
            ).await;
        }
    }};
    (component = $component: expr, $($arg:tt)*) => {{
        tracing::trace!(component = $component, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                None,
                $crate::tui::event::Severity::Trace,
                &format!($($arg)*)
            ).await;
        }
    }};
}

#[macro_export]
macro_rules! ui_debug {
    (component = $component: expr, id = $id: expr, $($arg:tt)*) => {{
        tracing::debug!(component = $component, id = %$id, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                Some($id.to_string()),
                $crate::tui::event::Severity::Debug,
                &format!($($arg)*)
            ).await;
        }
    }};
    (component = $component: expr, $($arg:tt)*) => {{
        tracing::debug!(component = $component, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                None,
                $crate::tui::event::Severity::Debug,
                &format!($($arg)*)
            ).await;
        }
    }};
}

#[macro_export]
macro_rules! ui_info {
    (component = $component: expr, id = $id: expr, $($arg:tt)*) => {{
        tracing::info!(component = $component, id = %$id, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                Some($id.to_string()),
                $crate::tui::event::Severity::Info,
                &format!($($arg)*)
            ).await;
        }
    }};
    (component = $component: expr, $($arg:tt)*) => {{
        tracing::info!(component = $component, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                None,
                $crate::tui::event::Severity::Info,
                &format!($($arg)*)
            ).await;
        }
    }};
}

#[macro_export]
macro_rules! ui_warn {
    (component = $component: expr, id = $id: expr, $($arg:tt)*) => {{
        tracing::warn!(component = $component, id = %$id, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                Some($id.to_string()),
                $crate::tui::event::Severity::Warn,
                &format!($($arg)*)
            ).await;
        }
    }};
    (component = $component: expr, $($arg:tt)*) => {{
        tracing::warn!(component = $component, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                None,
                $crate::tui::event::Severity::Warn,
                &format!($($arg)*)
            ).await;
        }
    }};
}

#[macro_export]
macro_rules! ui_error {
    (component = $component: expr, id = $id: expr, $($arg:tt)*) => {{
        tracing::error!(component = $component, id = %$id, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                Some($id.to_string()),
                $crate::tui::event::Severity::Error,
                &format!($($arg)*)
            ).await;
        }
    }};
    (component = $component: expr, $($arg:tt)*) => {{
        tracing::error!(component = $component, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                None,
                $crate::tui::event::Severity::Error,
                &format!($($arg)*)
            ).await;
        }
    }};
}

#[macro_export]
macro_rules! ui_fatal {
    (component = $component: expr, id = $id: expr, $($arg:tt)*) => {{
        tracing::error!(component = $component, id = %$id, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                Some($id.to_string()),
                $crate::tui::event::Severity::Fatal,
                &format!($($arg)*)
            ).await;
        }
    }};
    (component = $component: expr, $($arg:tt)*) => {{
        tracing::error!(component = $component, $($arg)*);
        if let Some(c) = $crate::tui::CONSOLE.get() {
            c.emit_diagnostic(
                $component,
                None,
                $crate::tui::event::Severity::Fatal,
                &format!($($arg)*)
            ).await;
        }
    }};
}

// (No global suppressions needed.)
