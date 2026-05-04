use snafu::Snafu;

/// Errors produced by the environment subsystem.
///
/// Covers failures from environment setup, command execution, storage access,
/// plugin invocation, and handlebars template rendering used by [`super::Command`].
#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum EnvironmentError {
    /// A propagated context-layer error (e.g. project/config issues).
    #[snafu(transparent)]
    Context {
        #[snafu(source(from(crate::context::ContextError, Box::new)))]
        source: Box<crate::context::ContextError>,
    },
    /// An opaque error surfaced by an environment implementation (plugin or builtin).
    #[snafu(transparent)]
    Implementation {
        source: Box<dyn snafu::Error + Send + Sync>,
    },
    /// A command executed inside the environment returned a non-zero exit status.
    #[snafu(display("command execution failed"))]
    Run,
    /// A propagated storage-layer error encountered during environment setup or I/O.
    #[snafu(transparent)]
    Storage {
        #[snafu(source(from(crate::storage::StorageError, Box::new)))]
        source: Box<crate::storage::StorageError>,
    },
    /// Handlebars template rendering failed while substituting command variables.
    #[snafu(display("failed to render substitution: {source}"))]
    Template { source: handlebars::RenderError },
}
