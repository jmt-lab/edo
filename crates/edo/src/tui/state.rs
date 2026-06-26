use std::collections::BTreeMap;

use jiff::Timestamp;

use crate::{
    context::Addr,
    tui::event::{Event, Task, TaskStatus},
};

/// Aggregate of the current state of the running progress
#[derive(Default)]
pub struct State {
    /// Root address used
    pub addr: Option<Addr>,
    /// Currently in-flight tasks keyed by component:id
    pub active: BTreeMap<String, Task>,
    /// Total transforms count
    pub total: usize,
    /// Tasks waiting for execution
    pub waiting: usize,
    /// Tasks that are in-flight
    pub in_flight: usize,
    /// Tasks that have reached a terminal state
    pub finished: usize,
    /// failed tasks
    pub failed: Vec<String>,
    /// True after finish
    pub done: bool,
    /// Final overall success flag
    pub ok: bool,
    /// Start time
    pub start: Timestamp,
}

impl State {
    pub fn apply(&mut self, event: &Event) {
        match event {
            Event::Header {
                addr, started_at, ..
            } => {
                self.addr = addr.clone();
                self.start = started_at.clone();
            }
            Event::StartBuild { addr, total } => {
                self.total += total;
                self.addr = Some(addr.clone());
            }
            Event::StartTask {
                component,
                id,
                status,
                operation,
                message,
            } => {
                let key = format!("{component}:{id}");
                match status {
                    TaskStatus::Cached | TaskStatus::Failed | TaskStatus::Success => {
                        self.finished += 1
                    }
                    TaskStatus::Running => self.in_flight += 1,
                    TaskStatus::Wait => self.waiting += 1,
                    _ => {}
                };
                self.active.insert(
                    key,
                    Task::builder()
                        .component(component.clone())
                        .id(id.clone())
                        .status(status.clone())
                        .operation(operation.clone())
                        .maybe_message(message.clone())
                        .status_since(Timestamp::now())
                        .build(),
                );
            }
            Event::UpdateTask {
                component,
                id,
                operation,
                status,
                message,
            } => {
                let key = format!("{component}:{id}");
                let mut should_remove = false;
                if let Some(task) = self.active.get_mut(&key) {
                    match (&task.status, status) {
                        (TaskStatus::Wait, TaskStatus::Running) => {
                            self.waiting -= 1;
                            self.in_flight += 1;
                        }
                        (TaskStatus::Wait, TaskStatus::Success | TaskStatus::Failed) => {
                            self.waiting -= 1;
                            self.finished += 1;
                            should_remove = true;
                        }
                        (TaskStatus::Wait, TaskStatus::Canceled) => {
                            self.waiting -= 1;
                            should_remove = true;
                        }
                        (TaskStatus::Running, TaskStatus::Success) => {
                            self.in_flight -= 1;
                            self.finished += 1;
                            should_remove = true;
                        }
                        (TaskStatus::Running, TaskStatus::Failed) => {
                            self.in_flight -= 1;
                            self.finished += 1;
                            should_remove = true;
                            self.failed.push(key.clone());
                        }
                        (TaskStatus::Running, TaskStatus::Canceled) => {
                            self.in_flight -= 1;
                            should_remove = true;
                        }
                        _ => {}
                    }
                    task.operation = operation.clone();
                    task.status = status.clone();
                    task.status_since = Timestamp::now();
                    task.message = message.clone();
                }
                if should_remove {
                    self.active.remove(&key);
                }
            }
            Event::BuildFinish => {
                self.done = true;
                self.ok = self.failed.is_empty();
            }
            _ => {}
        }
    }
}
