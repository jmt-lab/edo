//! Error types for the context module.
//!
//! All fallible operations in the context module return
//! [`ContextError`] via the [`ContextResult`](super::ContextResult) alias.
//! Variants use SNAFU's `#[snafu(display(...))]` attribute for formatting.

use snafu::Snafu;
use tracing_subscriber::util::TryInitError;

use super::Addr;

/// Enumerates all errors that can occur within the context module.
#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum ContextError {
    /// A required field was missing or had the wrong type.
    #[snafu(display("expected a field named '{field}' with a type of {type_}"))]
    Field {
        /// Name of the expected field.
        field: String,
        /// Expected type description.
        type_: String,
    },
    /// The user's home directory could not be determined.
    #[snafu(display("failed to find home directory"))]
    Home,
    /// An I/O operation failed.
    #[snafu(display("io error occured: {source}"))]
    Io {
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// Dependencies changed since the lockfile was generated.
    #[snafu(display("dependencies have changed, run edo update to update the lockfile"))]
    DependencyChange,
    /// TOML deserialization failed.
    #[snafu(display("failed to deserialize toml: {source}"))]
    Deserialize {
        /// The underlying TOML deserialization error.
        source: toml::de::Error,
    },
    /// Logging subsystem initialization failed.
    #[snafu(display("failed to initialize logging: {source}"))]
    Log {
        /// The underlying tracing initialization error.
        source: TryInitError,
    },
    /// The lockfile is missing resolution data for an address.
    #[snafu(display("lockfile is missing resolution data for: {addr}"))]
    MalformedLock {
        /// The address that has no lock entry.
        addr: Addr,
    },
    /// A configuration value could not be read as a node.
    #[snafu(display("could not read to a configuration node"))]
    Node,
    /// A node is missing one or more required keys.
    #[snafu(display("node is missing required keys {}", keys.join(", ")))]
    NodeMissingKeys {
        /// The list of missing key names.
        keys: Vec<String>,
    },
    /// A node is missing a kind definition.
    #[snafu(display("node is missing a kind definition"))]
    NodeNoKind,
    /// A node is missing a name.
    #[snafu(display("node is missing a name"))]
    NodeNoName,
    /// A node is missing an id.
    #[snafu(display("node is missing an id"))]
    NodeNoId,
    /// The block id could not be determined.
    #[snafu(display("could not determine block id"))]
    NoBlockId,
    /// The block is not an environment definition.
    #[snafu(display("block is not an environment definition"))]
    NotEnvironment,
    /// No environment was found for the given address.
    #[snafu(display("no environment found with addr '{addr}'"))]
    NoEnvironmentFound {
        /// The address that was looked up.
        addr: Addr,
    },
    /// No plugin is loaded for the given address.
    #[snafu(display("no plugin loaded with addr '{addr}'"))]
    NoPlugin {
        /// The plugin address that was not found.
        addr: Addr,
    },
    /// No loaded plugin supports the requested component kind.
    #[snafu(display("no plugin is loaded that supports a {component} of kind {kind}"))]
    NoProvider {
        /// The component type being requested.
        component: String,
        /// The kind discriminator that no plugin supports.
        kind: String,
    },
    /// The block is not a transform definition.
    #[snafu(display("block is not a transform definition"))]
    NotTransform,
    /// The id is not valid for a source definition.
    #[snafu(display("'{id}' is not a valid block id for a source definition"))]
    NotValidSource {
        /// The invalid source id.
        id: String,
    },
    /// The block is not a vendor definition.
    #[snafu(display("block is not a vendor definition"))]
    NotVendor,
    /// A plugin subsystem error.
    #[snafu(transparent)]
    Plugin {
        /// The underlying plugin error.
        source: crate::plugin::error::PluginError,
    },
    /// An environment subsystem error.
    #[snafu(transparent)]
    Environment {
        /// The underlying environment error.
        source: crate::environment::EnvironmentError,
    },
    /// A scheduler subsystem error.
    #[snafu(transparent)]
    Scheduler {
        /// The underlying scheduler error.
        #[snafu(source(from(crate::scheduler::error::SchedulerError, Box::new)))]
        source: Box<crate::scheduler::error::SchedulerError>,
    },
    /// JSON serialization failed.
    #[snafu(display("failed to serialize to json: {source}"))]
    Serialize {
        /// The underlying JSON serialization error.
        source: serde_json::Error,
    },
    /// A storage subsystem error.
    #[snafu(transparent)]
    Storage {
        /// The underlying storage error.
        #[snafu(source(from(crate::storage::StorageError, Box::new)))]
        source: Box<crate::storage::StorageError>,
    },
    /// A transform subsystem error.
    #[snafu(transparent)]
    Transform {
        /// The underlying transform error.
        source: crate::transform::TransformError,
    },
    /// A source subsystem error.
    #[snafu(transparent)]
    Source {
        /// The underlying source error.
        source: crate::source::SourceError,
    },
}
