//! Node representation within the scheduler execution graph.

use crate::context::Addr;
use std::sync::atomic::{AtomicU8, Ordering};

/// A node in the execution graph representing a single transform unit.
pub struct Node {
    /// The address identifying this transform in the project.
    pub addr: Addr,
    /// Atomic status byte encoding the current [`NodeStatus`].
    pub status: AtomicU8,
}

impl Node {
    /// Creates a new node with the given address in [`NodeStatus::Pending`] state.
    pub fn new(addr: &Addr) -> Self {
        Self {
            addr: addr.clone(),
            status: AtomicU8::new(NodeStatus::Pending as u8),
        }
    }

    /// Transitions this node to [`NodeStatus::Queued`].
    pub fn set_queued(&self) {
        self.status
            .store(NodeStatus::Queued as u8, Ordering::SeqCst);
    }

    /// Transitions this node to [`NodeStatus::Running`].
    pub fn set_running(&self) {
        self.status
            .store(NodeStatus::Running as u8, Ordering::SeqCst);
    }

    /// Transitions this node to [`NodeStatus::Failed`].
    pub fn set_failed(&self) {
        self.status
            .store(NodeStatus::Failed as u8, Ordering::SeqCst);
    }

    /// Transitions this node to [`NodeStatus::Success`].
    pub fn set_success(&self) {
        self.status
            .store(NodeStatus::Success as u8, Ordering::SeqCst);
    }

    /// Returns `true` if this node has not yet been queued or started.
    pub fn is_pending(&self) -> bool {
        let status = self.status();
        status == NodeStatus::Pending
    }

    /// Returns `true` if this node is waiting in the dispatch queue.
    pub fn is_queued(&self) -> bool {
        let status = self.status();
        status == NodeStatus::Queued
    }

    /// Returns `true` if this node's transform failed.
    pub fn is_failed(&self) -> bool {
        let status = self.status();
        status == NodeStatus::Failed
    }

    /// Returns `true` if this node has finished (either success or failure).
    pub fn is_done(&self) -> bool {
        let status = self.status();
        status == NodeStatus::Success || status == NodeStatus::Failed
    }

    /// Returns the current execution status of this node.
    pub fn status(&self) -> NodeStatus {
        NodeStatus::from(self.status.load(Ordering::SeqCst))
    }
}

/// Execution lifecycle state of a scheduler node.
#[derive(Debug, PartialEq, Eq)]
pub enum NodeStatus {
    /// Not yet scheduled for execution.
    Pending = 0,
    /// Placed in the dispatch queue awaiting a worker slot.
    Queued = 1,
    /// Currently being executed by a worker.
    Running = 2,
    /// Transform execution failed.
    Failed = 3,
    /// Transform execution completed successfully.
    Success = 4,
}

impl From<u8> for NodeStatus {
    fn from(value: u8) -> Self {
        match value {
            x if x == NodeStatus::Pending as u8 => NodeStatus::Pending,
            x if x == NodeStatus::Queued as u8 => NodeStatus::Queued,
            x if x == NodeStatus::Running as u8 => NodeStatus::Running,
            x if x == NodeStatus::Failed as u8 => NodeStatus::Failed,
            x if x == NodeStatus::Success as u8 => NodeStatus::Success,
            _ => NodeStatus::Failed,
        }
    }
}
