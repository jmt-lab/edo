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

#[cfg(test)]
mod tests {
    //! Pure unit tests for [`Node`] and [`NodeStatus`].
    //!
    //! These do not require a `Context` and therefore do not need to
    //! serialize on the `log_manager` tag — they can run in parallel.

    use super::*;
    use crate::context::Addr;
    use std::sync::Arc;

    fn make_addr() -> Addr {
        Addr::parse("//proj/node").expect("addr parse")
    }

    #[test]
    fn new_starts_pending() {
        let n = Node::new(&make_addr());
        assert_eq!(n.status(), NodeStatus::Pending);
        assert!(n.is_pending());
        assert!(!n.is_queued());
        assert!(!n.is_failed());
        assert!(!n.is_done());
    }

    #[test]
    fn set_queued_transitions_to_queued() {
        let n = Node::new(&make_addr());
        n.set_queued();
        assert_eq!(n.status(), NodeStatus::Queued);
        assert!(n.is_queued());
        assert!(!n.is_pending());
        assert!(!n.is_done());
    }

    #[test]
    fn set_running_transitions_to_running() {
        let n = Node::new(&make_addr());
        n.set_running();
        assert_eq!(n.status(), NodeStatus::Running);
        assert!(!n.is_pending());
        assert!(!n.is_queued());
        assert!(!n.is_failed());
        assert!(!n.is_done());
    }

    #[test]
    fn set_failed_transitions_to_failed_and_is_done() {
        let n = Node::new(&make_addr());
        n.set_failed();
        assert_eq!(n.status(), NodeStatus::Failed);
        assert!(n.is_failed());
        assert!(n.is_done());
    }

    #[test]
    fn set_success_transitions_to_success_and_is_done() {
        let n = Node::new(&make_addr());
        n.set_success();
        assert_eq!(n.status(), NodeStatus::Success);
        assert!(!n.is_failed());
        assert!(n.is_done());
    }

    #[test]
    fn node_status_from_u8_maps_all_valid_values() {
        assert_eq!(NodeStatus::from(0u8), NodeStatus::Pending);
        assert_eq!(NodeStatus::from(1u8), NodeStatus::Queued);
        assert_eq!(NodeStatus::from(2u8), NodeStatus::Running);
        assert_eq!(NodeStatus::from(3u8), NodeStatus::Failed);
        assert_eq!(NodeStatus::from(4u8), NodeStatus::Success);
    }

    #[test]
    fn node_status_from_u8_unknown_maps_to_failed() {
        // Any byte outside the valid discriminants is treated as a failure
        // so that state machines cannot get stuck in an impossible state.
        assert_eq!(NodeStatus::from(5u8), NodeStatus::Failed);
        assert_eq!(NodeStatus::from(99u8), NodeStatus::Failed);
        assert_eq!(NodeStatus::from(u8::MAX), NodeStatus::Failed);
    }

    /// Confirms the atomic is observable across threads (SeqCst ordering).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn status_visible_across_threads() {
        let node = Arc::new(Node::new(&make_addr()));
        let writer = node.clone();
        let join = tokio::spawn(async move {
            writer.set_running();
            // Yield to let the reader observe the intermediate state.
            tokio::task::yield_now().await;
            writer.set_success();
        });
        join.await.expect("writer task");
        assert_eq!(node.status(), NodeStatus::Success);
        assert!(node.is_done());
    }
}
