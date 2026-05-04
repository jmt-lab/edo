//! Scheduler module for DAG-based parallel transform execution.
//!
//! Orchestrates build tasks by constructing a dependency graph and executing
//! transforms in topological order with configurable concurrency.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use snafu::ResultExt;
use tokio::fs::create_dir_all;
use graph::Graph;
use crate::context::{Addr, Config};
use super::context::Context;

/// Error types for the scheduler subsystem.
pub mod error;
/// Interactive transform executor with error recovery.
pub mod execute;
/// DAG-based execution graph for parallel transform orchestration.
pub mod graph;
/// Node representation within the scheduler execution graph.
pub mod node;

type Result<T> = std::result::Result<T, error::SchedulerError>;

/// Parallel task scheduler that builds a dependency graph and executes transforms concurrently.
///
/// Wraps an inner state behind an `Arc` for cheap cloning across async tasks.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<Inner>,
}

impl Scheduler {
    /// Creates a new scheduler rooted at the given workspace path.
    ///
    /// Reads the `scheduler.workers` config key to determine concurrency (defaults to 8).
    /// Creates the workspace directory if it does not already exist.
    pub async fn new<P: AsRef<Path>>(path: P, config: &Config) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            create_dir_all(path).await.context(error::IoSnafu)?;
        }
        let workers = if let Some(node) = config.get("scheduler") {
            node.get("workers").and_then(|x| x.as_int())
        } else {
            None
        };
        Ok(Self {
            inner: Arc::new(Inner {
                workers: if let Some(workers) = workers {
                    workers as u64
                } else {
                    8
                },
                path: path.to_path_buf(),
            }),
        })
    }
}

impl Scheduler {
    /// Builds the dependency graph for the given address and executes all transforms.
    pub async fn run(&self, ctx: &Context, addr: &Addr) -> Result<()> {
        self.inner.run(ctx, addr).await
    }
}

struct Inner {
    path: PathBuf,
    workers: u64,
}

impl Inner {
    pub async fn run(&self, ctx: &Context, addr: &Addr) -> Result<()> {
        let mut graph = Graph::new(self.workers);
        graph.add(ctx, addr).await?;
        graph.fetch(ctx).await?;
        let graph_ref = Arc::new(graph);
        graph_ref.run(&self.path, ctx, addr).await?;
        Ok(())
    }
}
