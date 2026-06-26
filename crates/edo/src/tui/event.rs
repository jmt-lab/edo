use std::path::PathBuf;

use jiff::Timestamp;
use ratatui::{
    style::{Color, Style},
    text::{Line, Span},
};
use serde::{Deserialize, Serialize};

use crate::context::Addr;

static EDO_VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum Event {
    /// Header event happens on system load
    Header {
        /// Version of the build tool using edo
        version: semver::Version,
        /// Top level address being asked to build/run or whatever if provided
        addr: Option<Addr>,
        /// --arg key=value pairs
        args: Vec<(String, String)>,
        /// Wall-clock start time
        started_at: Timestamp,
    },
    /// Summary of the project emitted after ctx is fully loaded
    Summary {
        /// Absolute path the project root
        root: PathBuf,
        /// Number of transforms registered
        transforms: usize,
        /// Number of sources registered post lock resolution
        sources: usize,
        /// Number of environment farms
        farms: usize,
        /// True when the lockfile was reused
        locked: bool,
    },
    /// Start of a build
    StartBuild { addr: Addr, total: usize },
    /// Add a new running task onto the tui
    StartTask {
        /// Component this task represents
        component: String,
        /// ID unique to the component
        id: String,
        /// Status to start with
        status: TaskStatus,
        /// Operation the task is performing
        operation: String,
        // Any start of operation message
        message: Option<String>,
    },
    /// Update an existing task in the tui
    UpdateTask {
        /// Component of the task to update
        component: String,
        /// ID unique to the component
        id: String,
        /// Operation being performed
        operation: String,
        /// Status to update to
        status: TaskStatus,
        /// Optional message with the update
        message: Option<String>,
    },
    /// Diagnostic Message sent into the top log
    Diagnostic {
        /// Component of this diagnostic is coming from
        component: String,
        /// Optional ID
        id: Option<String>,
        /// Severity of the diagnostic
        severity: Severity,
        /// Message to send
        message: String,
    },
    /// Build Finished
    BuildFinish,
    /// Terminate the UI
    Terminate,
}

impl Event {
    pub fn to_lines(&self) -> Vec<Line> {
        match self {
            Self::Header {
                version,
                addr,
                args,
                started_at,
            } => {
                let mut lines = vec![Line::from(vec![
                    Span::styled("tool-version: ", Style::default().bold()),
                    Span::raw(version.to_string()),
                ])];
                if let Some(addr) = addr {
                    lines.push(Line::from(vec![
                        Span::styled("target: ", Style::default().bold()),
                        Span::raw(addr.to_string()),
                    ]));
                }
                if !args.is_empty() {
                    lines.push(Line::from(vec![Span::styled(
                        "args:",
                        Style::default().bold(),
                    )]))
                }
                for (key, value) in args {
                    lines.push(Line::from(vec![
                        Span::styled(format!(" {key}: "), Style::default().bold()),
                        Span::raw(value),
                    ]));
                }
                lines.push(Line::from(vec![
                    Span::styled("started: ", Style::default().bold()),
                    Span::raw(started_at.to_string()),
                ]));
                lines
            }
            Self::Summary {
                root,
                transforms,
                sources,
                farms,
                locked,
            } => vec![
                Line::from(vec![
                    Span::styled("project: ", Style::default().bold()),
                    Span::raw(root.to_string_lossy().to_string()),
                ]),
                Line::from(vec![
                    Span::styled("transforms: ", Style::default().bold()),
                    Span::raw(transforms.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("sources: ", Style::default().bold()),
                    Span::raw(sources.to_string()),
                ]),
                Line::from(vec![
                    Span::styled("environments: ", Style::default().bold()),
                    Span::raw(farms.to_string()),
                ]),
                Line::from(vec![if *locked {
                    Span::styled("lock match", Style::default().bold().fg(Color::Green))
                } else {
                    Span::styled("lock mismatch", Style::default().bold().fg(Color::Yellow))
                }]),
            ],
            Self::Diagnostic {
                component,
                id,
                severity,
                message,
            } => vec![Line::from(vec![
                severity.to_span(),
                Span::styled(
                    format!(
                        " [{component}{}]:",
                        if let Some(id) = id.as_ref() {
                            format!(":{id}")
                        } else {
                            "".to_string()
                        }
                    ),
                    Style::default().fg(Color::DarkGray).italic(),
                ),
                Span::raw(format!(" {message}")),
            ])],
            Self::StartTask {
                component,
                id,
                status,
                operation,
                message,
            } if *status == TaskStatus::Cached => vec![Line::from(vec![
                status.to_span(),
                Span::styled(
                    format!(" [{component}:{id}]({operation})"),
                    Style::default().fg(Color::DarkGray).italic(),
                ),
                if let Some(message) = message {
                    Span::raw(format!(": {message}"))
                } else {
                    Span::raw("")
                },
            ])],
            Self::UpdateTask {
                component,
                id,
                operation,
                status,
                message,
            } if matches!(
                status,
                TaskStatus::Canceled | TaskStatus::Success | TaskStatus::Failed
            ) =>
            {
                vec![Line::from(vec![
                    status.to_span(),
                    Span::styled(
                        format!(" [{component}:{id}]({operation})"),
                        Style::default().fg(Color::DarkGray).italic(),
                    ),
                    if let Some(message) = message {
                        Span::raw(format!(": {message}"))
                    } else {
                        Span::raw("")
                    },
                ])]
            }
            _ => vec![],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Severity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl Severity {
    pub fn to_span(&self) -> Span {
        match self {
            Self::Trace => Span::styled("trace", Style::default().fg(Color::DarkGray)),
            Self::Debug => Span::styled("debug", Style::default().fg(Color::Gray)),
            Self::Info => Span::styled("info", Style::default().fg(Color::Blue)),
            Self::Warn => Span::styled("warning", Style::default().fg(Color::Yellow)),
            Self::Error => Span::styled("error", Style::default().fg(Color::Red)),
            Self::Fatal => Span::styled("fatal", Style::default().bold().fg(Color::LightRed)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Wait,
    Running,
    Failed,
    Success,
    Canceled,
    Cached,
}

impl TaskStatus {
    pub fn to_span(&self) -> Span {
        match self {
            Self::Wait => Span::styled("waiting", Style::default().fg(Color::DarkGray)),
            Self::Running => Span::styled("running", Style::default().fg(Color::Blue)),
            Self::Failed => Span::styled("failed", Style::default().fg(Color::Red)),
            Self::Success => Span::styled("success", Style::default().fg(Color::Green)),
            Self::Canceled => {
                Span::styled("canceled", Style::default().fg(Color::Gray).crossed_out())
            }
            Self::Cached => Span::styled("cached", Style::default().fg(Color::Gray).italic()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, bon::Builder)]
pub struct Task {
    pub component: String,
    pub id: String,
    pub operation: String,
    pub status: TaskStatus,
    pub status_since: Timestamp,
    pub message: Option<String>,
}

impl Task {
    pub fn to_line(&self) -> Line {
        let span = Timestamp::now() - self.status_since;
        Line::from(vec![
            Span::styled(
                format!("({}) ", span.to_string()),
                Style::default().fg(Color::Gray).italic(),
            ),
            self.status.to_span(),
            Span::styled(
                format!(" [{}:{}]({})", self.component, self.id, self.operation),
                Style::default().fg(Color::DarkGray).italic(),
            ),
            if let Some(message) = self.message.as_ref() {
                Span::raw(format!(": {message}"))
            } else {
                Span::raw("")
            },
        ])
    }
}
