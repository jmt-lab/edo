use crate::context::{Addr, Handle, Log};
use crate::environment::Environment;
use crate::storage::{Artifact, Id};
use arc_handle::arc_handle;
use async_trait::async_trait;
use std::path::PathBuf;

pub type TransformResult<T> = std::result::Result<T, error::TransformError>;
pub use error::TransformError;

/// A Transform represents an action that transforms a source artifact into another usually
/// build artifact
#[arc_handle]
#[async_trait]
pub trait Transform {
    /// Returns the address of the environment farm to use
    async fn environment(&self) -> TransformResult<Addr>;
    /// Return the transforms unique id that will represent its output
    async fn get_unique_id(&self, ctx: &Handle) -> TransformResult<Id>;
    /// Returns all dependent transforms of this one
    async fn depends(&self) -> TransformResult<Vec<Addr>>;
    /// Prepare the transform by fetching all sources and dependent artifacts
    async fn prepare(&self, log: &Log, ctx: &Handle) -> TransformResult<()>;
    /// Stage all needed files into the environment
    async fn stage(&self, log: &Log, ctx: &Handle, env: &Environment) -> TransformResult<()>;
    /// Perform the transformation
    async fn transform(&self, log: &Log, ctx: &Handle, env: &Environment) -> TransformStatus;
    /// Can a user enter a shell if this transform fails
    fn can_shell(&self) -> bool;
    /// Open a shell in the environment at the appropriate location
    fn shell(&self, env: &Environment) -> TransformResult<()>;
}

pub enum TransformStatus {
    Success(Artifact),
    Retryable(Option<PathBuf>, error::TransformError),
    Failed(Option<PathBuf>, error::TransformError),
}

pub mod error {
    use snafu::Snafu;

    #[derive(Snafu, Debug)]
    #[snafu(visibility(pub))]
    pub enum TransformError {
        #[snafu(transparent)]
        Implementation {
            source: Box<dyn snafu::Error + Send + Sync>,
        },
        #[snafu(transparent)]
        Context {
            #[snafu(source(from(crate::context::ContextError, Box::new)))]
            source: Box<crate::context::ContextError>,
        },
        #[snafu(transparent)]
        Environment {
            #[snafu(source(from(crate::environment::EnvironmentError, Box::new)))]
            source: Box<crate::environment::EnvironmentError>,
        },
        #[snafu(transparent)]
        Source {
            #[snafu(source(from(crate::source::SourceError, Box::new)))]
            source: Box<crate::source::SourceError>,
        },
        #[snafu(transparent)]
        Storage {
            #[snafu(source(from(crate::storage::StorageError, Box::new)))]
            source: Box<crate::storage::StorageError>,
        },
    }
}

#[macro_export]
macro_rules! transform_err {
    ($expr: expr) => {
        match $expr {
            Ok(data) => data,
            Err(e) => {
                error!("wrapped error occured: {e}");
                return TransformStatus::Failed(None, e.into());
            }
        }
    };
}

pub use transform_err;
