//! Build context — the central coordinator for an edo build session.
//!
//! The [`Context`] struct ties together configuration, storage, logging,
//! scheduling, plugins, transforms, and environment farms. It is created
//! once per invocation via [`Context::init`] and threaded through the
//! entire build pipeline.
//!
//! Sub-modules provide supporting types:
//! - Addressing — hierarchical [`Addr`] identifiers
//! - Configuration — user-level [`Config`] and the [`Definable`] traits
//! - Errors — [`ContextError`] and the [`ContextResult`] alias
//! - Handle — read-only [`Handle`] passed to transforms
//! - Lock — dependency lock file ([`Lock`])
//! - Logging — per-task [`Log`] files and [`LogManager`] tracing setup
//! - Node — generic data tree ([`Node`], [`Data`], [`Component`])
//! - Schema — TOML schema deserialization
//! - Builder — project loading and dependency resolution ([`Project`])

use std::collections::{BTreeMap, HashMap};
use std::env::current_dir;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use dashmap::DashMap;
use snafu::{OptionExt, ResultExt, ensure};
use tokio::fs::create_dir_all;
use tracing::Instrument;
use crate::{plugin::WasmPlugin, storage::{Backend, LocalBackend, Storage}};
use super::{
    environment::Farm,
    plugin::Plugin,
    scheduler::Scheduler,
    source::{Source, Vendor},
    transform::Transform,
};

mod address;
mod builder;
mod config;
pub mod error;
mod handle;
mod lock;
mod log;
mod logmgr;
mod node;
mod schema;
/// Re-exports [`Addr`] and [`Addressable`].
pub use address::*;
/// Re-exports [`Project`] and the `non_configurable` macros.
pub use builder::*;
/// Re-exports [`Config`], [`Definable`], [`DefinableNoContext`], and [`NonConfigurable`].
pub use config::*;
/// Re-exports [`ContextError`] at the module level.
pub use error::ContextError;
/// Re-exports [`Handle`].
pub use handle::*;
/// Re-exports [`Lock`].
pub use lock::*;
/// Re-exports [`Log`].
pub use log::*;
/// Re-exports [`LogManager`], [`LogVerbosity`], and logging helpers.
pub use logmgr::*;
/// Re-exports [`Node`], [`Data`], [`Component`], [`FromNode`], and [`FromNodeNoContext`].
pub use node::*;

/// Convenience alias for `Result<T, ContextError>`.
pub type ContextResult<T> = std::result::Result<T, error::ContextError>;

type ArcMap<K, V> = Arc<DashMap<K, V>>;

/// Default subdirectory name for edo's working data (`.edo`).
const DEFAULT_PATH: &str = ".edo";

/// Central coordinator for an edo build session.
///
/// Holds references to configuration, storage, logging, scheduling, and all
/// registered plugins, transforms, and environment farms. Created once via
/// [`Context::init`] and shared (via `Clone`) throughout the build.
#[derive(Clone)]
pub struct Context {
    /// Project directory
    project_dir: PathBuf,
    /// Loaded Shared Configuration
    config: Config,
    /// Storage Manager
    storage: Storage,
    /// Log Manager
    log: LogManager,
    /// Execution Scheduler
    scheduler: Scheduler,
    /// Loaded Plugins
    plugins: ArcMap<Addr, Plugin>,
    /// Registered Transforms
    transforms: ArcMap<Addr, Transform>,
    /// Registered Farms
    farms: ArcMap<Addr, Farm>,
    /// Command Line Arguments
    args: HashMap<String, String>,
}

unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    /// Initializes a new build context, setting up logging, configuration,
    /// storage, and the execution scheduler.
    pub async fn init<ProjectPath, ConfigPath>(
        path: Option<ProjectPath>,
        config: Option<ConfigPath>,
        args: HashMap<String, String>,
        verbosity: LogVerbosity,
    ) -> ContextResult<Self>
    where
        ProjectPath: AsRef<Path>,
        ConfigPath: AsRef<Path>,
    {
        let project_dir = current_dir().context(error::IoSnafu)?;
        let path = if let Some(path) = path.as_ref() {
            path.as_ref().to_path_buf()
        } else {
            project_dir.join(DEFAULT_PATH)
        };
        if !path.exists() {
            create_dir_all(&path).await.context(error::IoSnafu)?;
        }
        // Logs should be in a project specific folder, so they
        // do not clash with other project workspaces.
        let log_path = path.join("logs");
        let log = LogManager::init(&log_path, verbosity).await?;
        // Load the configuration
        let config = Config::load(config).await?;
        // Initialize the storage with the default local cache
        let storage = Storage::init(&Backend::new(
            LocalBackend::new(
                &Addr::parse("//edo-local-cache")?,
                &Node::new_definition(
                    "storage",
                    "local",
                    "edo-local-cache",
                    BTreeMap::from([(
                        "path".to_string(),
                        Node::new_string(path.join("storage").to_string_lossy().to_string()),
                    )]),
                ),
                &config,
            )
            .await?,
        ))
        .await?;

        // Create the initial context
        let ctx = Context {
            project_dir: project_dir.clone(),
            config: config.clone(),
            args,
            log: log.clone(),
            storage,
            scheduler: Scheduler::new(&path.join("env"), &config).await?,
            farms: Arc::new(DashMap::new()),
            plugins: Arc::new(DashMap::new()),
            transforms: Arc::new(DashMap::new()),
        };
        Ok(ctx.clone())
    }

    /// Loads the project from the current directory, resolving dependencies
    /// and registering all components.
    pub async fn load_project(&self, error_on_lock: bool) -> ContextResult<()> {
        Project::load(&self.project_dir, self, error_on_lock).await?;
        Ok(())
    }

    /// Creates a read-only [`Handle`] snapshot for use by transforms during execution.
    pub fn get_handle(&self) -> Handle {
        Handle::new(
            self.log.clone(),
            self.storage.clone(),
            self
                .transforms
                .iter()
                .map(|x| (x.key().clone(), x.value().clone()))
                .collect(),
            self
                .farms
                .iter()
                .map(|x| (x.key().clone(), x.value().clone()))
                .collect(),
            self.args.clone(),
        )
    }

    /// Returns a reference to the loaded configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns a reference to the storage manager.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Returns a reference to the log manager.
    pub fn log(&self) -> &LogManager {
        &self.log
    }

    /// Returns a reference to the execution scheduler.
    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// Prints all registered transform addresses to stdout.
    pub fn print_transforms(&self) {
        for addr in self.transforms.iter() {
            println!("{}", addr.key());
        }
    }

    /// Finds a plugin that supports the given `component` kind from the node.
    pub async fn find_plugin(&self, component: Component, node: &Node) -> ContextResult<Plugin> {
        let kind = node.get_kind().unwrap();
        if let Some((plugin, kind)) = kind.split_once(':') {
            let paddr = Addr::parse(plugin)?;
            let plugin = self
                .plugins
                .get(&paddr)
                .context(error::NoPluginSnafu { addr: paddr })?;
            node.set_kind(kind.to_string());
            ensure!(
                plugin
                    .supports(self, component.clone(), kind.to_string())
                    .await?,
                error::NoProviderSnafu {
                    component: component.to_string(),
                    kind
                }
            );
            Ok(plugin.value().clone())
        } else {
            for plugin in self.plugins.iter() {
                if plugin
                    .supports(self, component.clone(), kind.clone())
                    .await?
                {
                    return Ok(plugin.value().clone());
                }
            }
            error::NoProviderSnafu {
                component: component.to_string(),
                kind,
            }
            .fail()
        }
    }

    /// Returns the plugin registered at the given address, if any.
    pub fn get_plugin(&self, addr: &Addr) -> Option<Plugin> {
        self.plugins.get(addr).map(|x| x.value().clone())
    }

    /// Registers a pre-constructed plugin, fetching and setting it up before insertion.
    pub async fn add_preloaded_plugin(&self, addr: &Addr, plugin: &Plugin) -> ContextResult<()> {
        let log = self.log.create("init").await?;
        log.set_subject(&addr.to_string());
        plugin.fetch(&log, self.storage()).await?;
        plugin.setup(&log, self.storage()).await?;
        self.plugins.insert(addr.clone(), plugin.clone());
        Ok(())
    }

    /// Creates a new WASM plugin from the given node, fetches and sets it up, then registers it.
    pub async fn add_plugin(&self, addr: &Addr, node: &Node) -> ContextResult<()> {
        // Plugins cannot add other plugins so this is a discrete switch operation
        debug!(
            section = "context",
            component = "context",
            "adding a plugin {addr}"
        );
        let plugin = Plugin::new(WasmPlugin::from_node(addr, node, self).await?);
        let log = self.log.create("init").await?;
        log.set_subject(&addr.to_string());
        plugin.fetch(&log, self.storage()).await?;
        plugin.setup(&log, self.storage()).await?;
        self.plugins.insert(addr.clone(), plugin);
        Ok(())
    }

    /// Returns the transform registered at the given address, if any.
    pub fn get_transform(&self, addr: &Addr) -> Option<Transform> {
        self.transforms.get(addr).map(|x| x.value().clone())
    }

    /// Registers a storage cache backend, routing it to build, output, or source cache
    /// based on the address.
    pub async fn add_cache(&self, addr: &Addr, node: &Node) -> ContextResult<()> {
        debug!(
            section = "context",
            component = "context",
            "adding a storage backend {addr}"
        );
        let kind = node.get_kind().unwrap();
        let backend = if kind == "local" || kind == "edo:local" {
            Backend::new(LocalBackend::new(addr, node, self.config()).await?)
        } else {
            let plugin = self.find_plugin(Component::StorageBackend, node).await?;
            plugin.create_storage(addr, node, self).await?
        };
        let addr_s = addr.to_string();
        if addr_s == "//edo-build-cache" {
            // This is a build cache so add it
            self.storage().set_build(&backend).await;
        } else if addr_s == "//edo-output-cache" {
            // This is an output cache so add it
            self.storage().set_output(&backend).await;
        } else {
            // This is a source cache
            self.storage()
                .add_source_cache(addr_s.as_str(), &backend)
                .await;
        }
        Ok(())
    }

    /// Creates and registers a transform from the given node using the appropriate plugin.
    pub async fn add_transform(&self, addr: &Addr, node: &Node) -> ContextResult<()> {
        debug!(
            section = "context",
            component = "context",
            "adding a transform {addr}"
        );
        let plugin = self.find_plugin(Component::Transform, node).await?;
        self.transforms.insert(
            addr.clone(),
            plugin.create_transform(addr, node, self).await?,
        );
        Ok(())
    }

    /// Removes stale local storage entries for all registered transforms.
    pub async fn prune(&self) -> ContextResult<()> {
        let handle = self.get_handle();
        for transform in self.transforms.iter() {
            let id = transform.get_unique_id(&handle).await?;
            self.storage().prune_local(&id).await?;
        }
        Ok(())
    }

    /// Returns the environment farm registered at the given address, if any.
    pub fn get_farm(&self, addr: &Addr) -> Option<Farm> {
        self.farms.get(addr).map(|x| x.value().clone())
    }

    /// Creates and registers an environment farm from the given node using the appropriate plugin.
    pub async fn add_farm(&self, addr: &Addr, node: &Node) -> ContextResult<()> {
        debug!(
            section = "context",
            component = "context",
            "adding a farm {addr}"
        );
        let plugin = self.find_plugin(Component::Environment, node).await?;
        // If we get here use the core plugin
        self.farms
            .insert(addr.clone(), plugin.create_farm(addr, node, self).await?);
        Ok(())
    }

    /// Creates a source fetcher from the given node using the appropriate plugin.
    pub async fn add_source(&self, addr: &Addr, node: &Node) -> ContextResult<Source> {
        debug!(
            section = "context",
            component = "context",
            "adding a source {addr}"
        );
        let plugin = self.find_plugin(Component::Source, node).await?;
        let result = plugin.create_source(addr, node, self).await?;
        Ok(result)
    }

    /// Creates a dependency vendor from the given node using the appropriate plugin.
    pub async fn add_vendor(&self, addr: &Addr, node: &Node) -> ContextResult<Vendor> {
        let plugin = self.find_plugin(Component::Vendor, node).await?;
        let result = plugin.create_vendor(addr, node, self).await?;
        Ok(result)
    }

    /// Returns a reference to the command-line arguments map.
    pub fn args(&self) -> &HashMap<String, String> {
        &self.args
    }

    async fn setup_environments(&self) -> ContextResult<()> {
        // Run the initial setup for environments
        let log = self.log.create("setup").await?;
        log.set_subject("environment-setup");
        for entry in self.farms.iter() {
            entry
                .setup(&log, self.storage())
                .instrument(info_span!(
                    target: "context",
                    "setting up environment",
                    addr = entry.key().to_string()
                ))
                .await?;
        }
        Ok(())
    }

    /// Sets up environments and executes the build for the given transform address.
    pub async fn run(&self, addr: &Addr) -> ContextResult<()> {
        self.setup_environments().await?;
        self.scheduler().run(self, addr).await?;
        Ok(())
    }
}
