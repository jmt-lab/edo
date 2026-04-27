//! Read-only handle passed to transforms during execution.
//!
//! A [`Handle`] is a snapshot of the build context that gives transforms
//! access to logging, storage, other transforms, environment farms, and
//! command-line arguments without holding a reference to the full
//! [`Context`](super::Context).

use std::collections::HashMap;
use std::path::Path;
use snafu::OptionExt;
use super::{error, Addr, ContextResult, Log, LogManager};
use crate::{
    environment::{Environment, Farm},
    storage::Storage,
    transform::Transform,
};

/// A handle is passed to transforms where it needs to look up
/// things in the transform state.
#[derive(Clone)]
pub struct Handle {
    log: LogManager,
    storage: Storage,
    transforms: HashMap<Addr, Transform>,
    farms: HashMap<Addr, Farm>,
    args: HashMap<String, String>,
}

unsafe impl Send for Handle {}
unsafe impl Sync for Handle {}

impl Handle {
    /// Creates a new `Handle` with the given components.
    pub fn new(
        log: LogManager,
        storage: Storage,
        transforms: HashMap<Addr, Transform>,
        farms: HashMap<Addr, Farm>,
        args: HashMap<String, String>,
    ) -> Self {
        Self {
            log,
            storage,
            transforms,
            farms,
            args,
        }
    }

    /// Returns a reference to the log manager.
    pub fn log(&self) -> &LogManager {
        &self.log
    }

    /// Returns a reference to the storage backend.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Looks up a transform by address, returning a clone if found.
    pub fn get(&self, addr: &Addr) -> Option<Transform> {
        self.transforms.get(addr).cloned()
    }

    /// Returns a reference to the full transforms map.
    pub fn transforms(&self) -> &HashMap<Addr, Transform> {
        &self.transforms
    }

    /// Returns a reference to the command-line arguments map.
    pub fn args(&self) -> &HashMap<String, String> {
        &self.args
    }

    /// Creates a new build environment from the farm registered at `addr`.
    pub async fn create_environment(
        &self,
        log: &Log,
        addr: &Addr,
        path: &Path,
    ) -> ContextResult<Environment> {
        let farm = self
            .farms
            .get(addr)
            .context(error::NoEnvironmentFoundSnafu { addr: addr.clone() })?;
        let env = farm.create(log, path).await?;
        Ok(env)
    }
}
