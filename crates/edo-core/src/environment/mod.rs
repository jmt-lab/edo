use super::storage::Id;
use crate::context::Log;
use crate::util::{Reader, Writer};
use arc_handle::arc_handle;
use async_trait::async_trait;
use std::path::{Path, PathBuf};

use super::storage::Storage;

mod command;
pub mod error;
mod farm;

pub use command::*;
pub use error::EnvironmentError;
pub use farm::*;

pub type EnvResult<T> = std::result::Result<T, error::EnvironmentError>;

/// An Environment represents where a transform is executed and generally outside of local environments provide some level of sandboxing
/// and isolation.
#[arc_handle]
#[async_trait]
pub trait Environment {
    /// Expand the provided path to a canonicalized absolute path inside of an environment
    async fn expand(&self, path: &Path) -> EnvResult<PathBuf>;
    /// Create a directory inside of the environmet
    async fn create_dir(&self, path: &Path) -> EnvResult<()>;
    /// Set an environment variable
    async fn set_env(&self, key: &str, value: &str) -> EnvResult<()>;
    /// Get an environment variable
    async fn get_env(&self, key: &str) -> Option<String>;
    /// Setup the environment for execution
    async fn setup(&self, log: &Log, storage: &Storage) -> EnvResult<()>;
    /// Spin the environment up
    async fn up(&self, log: &Log) -> EnvResult<()>;
    /// Spin the environment down
    async fn down(&self, log: &Log) -> EnvResult<()>;
    /// Cleanup the environment
    async fn clean(&self, log: &Log) -> EnvResult<()>;
    /// Write a file into the environment from a given reader
    async fn write(&self, path: &Path, reader: Reader) -> EnvResult<()>;
    /// Unpack an archive into the environment from a given reader
    async fn unpack(&self, path: &Path, reader: Reader) -> EnvResult<()>;
    /// Read or archive a path in the environment to a given writer
    async fn read(&self, path: &Path, writer: Writer) -> EnvResult<()>;
    /// Run a single command in the environment
    async fn cmd(&self, log: &Log, id: &Id, path: &Path, command: &str) -> EnvResult<bool>;
    /// Run a deferred command in the environment
    async fn run(&self, log: &Log, id: &Id, path: &Path, command: &Command) -> EnvResult<bool>;
    /// Open a shell in the environment
    fn shell(&self, path: &Path) -> EnvResult<()>;
}

impl Environment {
    pub fn defer_cmd(&self, log: &Log, id: &Id) -> Command {
        Command::new(log, id, self)
    }
}
