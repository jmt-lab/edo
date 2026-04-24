use crate::context::Log;
use crate::environment::Environment;
use crate::storage::{Artifact, Id, Storage};
use arc_handle::arc_handle;
use async_trait::async_trait;
use std::path::Path;

mod error;
mod require;
mod resolver;
mod vendor;
mod version;

pub type SourceResult<T> = std::result::Result<T, error::SourceError>;
pub use error::SourceError;
pub use require::*;
pub use resolver::*;
pub use vendor::*;
pub use version::*;

/// A Source represents source code whether locally in project or from an external source
#[arc_handle]
#[async_trait]
pub trait Source {
    /// The unique id for this source
    async fn get_unique_id(&self) -> SourceResult<Id>;
    /// Fetch the given source to storage
    async fn fetch(&self, log: &Log, storage: &Storage) -> SourceResult<Artifact>;
    /// Stage the source into the given environment and path
    async fn stage(
        &self,
        log: &Log,
        storage: &Storage,
        env: &Environment,
        path: &Path,
    ) -> SourceResult<()>;
}

impl Source {
    /// Check the cache if this source already exists, and only if it does not
    /// call fetch to get the artifact. Use this in most cases instead of calling
    /// fetch() as fetch will ALWAYS repull the source.
    pub async fn cache(&self, log: &Log, storage: &Storage) -> SourceResult<Artifact> {
        // Now we want to check if our caches already have this artifact
        let id = self.get_unique_id().await?;
        // See if our storage can find this source artifact already
        // Note: we use fetch_source because we want to ensure when this is called
        // the artifact is in the local cache.
        if let Some(artifact) = storage.fetch_source(&id).await? {
            return Ok(artifact.clone());
        }
        // Otherwise perform the fetch
        self.fetch(log, storage).await
    }
}
