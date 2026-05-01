//! Project loading and build orchestration.
//!
//! This module contains [`Project`], which walks a directory tree for `edo.toml`
//! files, resolves dependencies through vendors, manages the lock file, and
//! registers plugins, environments, and transforms with the [`super::Context`].
//! It also re-exports the [`non_configurable!`] and
//! [`non_configurable_no_context!`] convenience macros.

use super::Context;
use super::address::Addr;
use super::lock::Lock;
use super::{ContextResult as Result, FromNode, Node, error};
use crate::context::schema::Schema;
use crate::source::{Dependency, Resolver};
use snafu::{OptionExt, ResultExt};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, read, read_dir};
use std::path::{Path, PathBuf};

/// Intermediate representation of a loaded edo project.
///
/// Holds the parsed configuration nodes collected from `edo.toml` files before
/// they are resolved and registered with the [`Context`].
pub struct Project {
    project_path: PathBuf,
    source_caches: BTreeMap<Addr, Node>,
    build_cache: Option<Node>,
    output_cache: Option<Node>,
    vendors: BTreeMap<Addr, Node>,
    environments: BTreeMap<Addr, Node>,
    transforms: BTreeMap<Addr, Node>,
    need_resolution: BTreeMap<Addr, Node>,
}

fn handle_sources(namespace: &Addr, node: &Node, sources: &BTreeMap<Addr, Node>) -> Result<Node> {
    let id = node.get_id().context(error::NodeSnafu)?;
    let kind = node.get_kind().context(error::NodeSnafu)?;
    let name = node.get_name().context(error::NodeSnafu)?;
    let mut table = node.get_table().context(error::NodeSnafu)?;

    if let Some(src) = table.get("source") {
        if let Some(addr) = src.as_string() {
            let caddr = if addr.starts_with("//") {
                Addr::parse(&addr)?
            } else {
                namespace.join(&addr)
            };
            info!("checking for source at: {caddr}");
            table.insert("source".into(), Node::new_string(caddr.to_string()));
        } else if let Some(list) = src.as_list() {
            let mut items = Vec::new();
            for item in list.iter() {
                let addr = item.as_string().context(error::FieldSnafu {
                    field: "requires",
                    type_: "array of string / string",
                })?;
                let caddr = if addr.starts_with("//") {
                    Addr::parse(&addr)?
                } else {
                    namespace.join(&addr)
                };
                items.push(Node::new_string(caddr.to_string()));
            }
            table.insert("source".into(), Node::new_list(items));
        }
    }

    Ok(Node::new_definition(&id, &kind, &name, table))
}

impl Project {
    fn calculate_digest(&self) -> Result<String> {
        let mut hasher = blake3::Hasher::new();
        for (key, value) in self.need_resolution.iter() {
            hasher.update(key.to_string().as_bytes());
            let bytes = serde_json::to_vec(value).context(error::SerializeSnafu)?;
            hasher.update(bytes.as_slice());
        }
        let digest = hasher.finalize();
        Ok(base16::encode_lower(digest.as_bytes()))
    }

    /// Loads all `edo.toml` files under `path`, resolves dependencies, and registers
    /// plugins, environments, and transforms with the given [`Context`].
    pub async fn load<P: AsRef<Path>>(path: P, ctx: &Context, error_on_lock: bool) -> Result<()> {
        let mut project = Self {
            project_path: path.as_ref().to_path_buf(),
            source_caches: BTreeMap::new(),
            build_cache: None,
            output_cache: None,
            vendors: BTreeMap::new(),
            environments: BTreeMap::new(),
            transforms: BTreeMap::new(),
            need_resolution: BTreeMap::new(),
        };
        let mut sources = BTreeMap::new();
        project.walk(&Addr::default(), path.as_ref(), &mut sources)?;
        project.resolve_sources(&sources)?;
        project.build(ctx, error_on_lock).await?;
        Ok(())
    }

    fn walk(
        &mut self,
        namespace: &Addr,
        directory: &Path,
        sources: &mut BTreeMap<Addr, Node>,
    ) -> Result<()> {
        let read = read_dir(directory).context(error::IoSnafu)?;
        for entry in read {
            let entry = entry.context(error::IoSnafu)?;
            let path = entry.path();
            if path.is_file() && path.file_name().and_then(|x| x.to_str()).unwrap() == "edo.toml" {
                // This is a barkml defined build file
                sources.extend(self.load_toml(namespace, &path)?);
            } else if path.is_dir() {
                let dir_name = path.file_name().and_then(|x| x.to_str()).unwrap();
                let addr = namespace.join(dir_name);
                self.walk(&addr, &path, sources)?;
            }
        }
        Ok(())
    }

    fn resolve_sources(&mut self, sources: &BTreeMap<Addr, Node>) -> Result<()> {
        for (name, node) in self
            .environments
            .iter_mut()
            .chain(self.transforms.iter_mut())
        {
            debug!(component = "project", "mapping sources for element {name}");
            let mut table = node.get_table().context(error::FieldSnafu {
                field: "environment",
                type_: "table",
            })?;
            if let Some(source) = table.get("source") {
                if let Some(list) = source.as_list() {
                    let mut items = Vec::new();
                    for entry in list.iter() {
                        let addr = Addr::parse(&entry.as_string().context(error::FieldSnafu {
                            field: "source",
                            type_: "string",
                        })?)?;
                        items.push(
                            sources
                                .get(&addr)
                                .context(error::NotValidSourceSnafu {
                                    id: addr.to_string(),
                                })?
                                .clone(),
                        );
                    }
                    table.insert("source".to_string(), Node::new_list(items));
                } else {
                    let mut addr =
                        Addr::parse(&source.as_string().context(error::FieldSnafu {
                            field: "source",
                            type_: "string",
                        })?)?;
                    table.insert(
                        "source".to_string(),
                        sources
                            .get(&addr)
                            .context(error::NotValidSourceSnafu {
                                id: addr.to_string(),
                            })?
                            .clone(),
                    );
                }
            }
            node.set_table(table);
        }
        Ok(())
    }

    fn load_toml(&mut self, namespace: &Addr, file: &Path) -> Result<BTreeMap<Addr, Node>> {
        debug!(component = "project", "loading transforms from {file:?}");
        let config_bytes = read(file).context(error::IoSnafu)?;
        let config: Schema = toml::from_slice(&config_bytes).context(error::DeserializeSnafu)?;
        match config {
            Schema::V1(config) => {
                let mut sources = BTreeMap::new();
                for (name, node) in config.get_sources()? {
                    let addr = namespace.join(&name);
                    sources.insert(addr, node);
                }
                for (name, node) in config.get_requires()? {
                    let addr = namespace.join(&name);
                    self.need_resolution.insert(addr.clone(), node.clone());
                    sources.insert(addr, node);
                }
                for (name, node) in config.get_source_caches()? {
                    let addr = namespace.join(&name);
                    self.source_caches.insert(addr, node.clone());
                }
                self.build_cache = config.get_build_cache()?;
                self.output_cache = config.get_output_cache()?;
                for (name, node) in config.get_environments()? {
                    let addr = namespace.join(&name);
                    let cnode = handle_sources(namespace, &node, &sources)?;
                    self.environments.insert(addr, cnode);
                }
                for (name, node) in config.get_transforms()? {
                    let addr = namespace.join(&name);
                    let cnode = handle_sources(namespace, &node, &sources)?;
                    self.transforms.insert(addr, cnode);
                }
                for (name, node) in config.get_vendors()? {
                    let addr = namespace.join(&name);
                    self.vendors.insert(addr, node);
                }
                return Ok(sources);
            }
        }
    }

    /// Resolves dependencies, registers plugins/environments/transforms, and
    /// writes the lock file.
    pub async fn build(&mut self, ctx: &Context, error_on_lock: bool) -> Result<()> {
        // Calculate the digest of the project configuration
        let digest = self.calculate_digest()?;
        // Check for an existing lockfile
        let lock_file = self.project_path.join("edo.lock.json");
        if lock_file.exists() {
            let mut file = File::open(&lock_file).context(error::IoSnafu)?;
            let lock: Lock = serde_json::from_reader(&mut file).context(error::SerializeSnafu)?;
            // Now check if the digests match, if so then we should use the lockfile to resolve our unresolved nodes
            if lock.digest() == digest {
                info!(target: "project", "no changes detected in project, reusing lock resolution file");
                for (addr, node) in self.need_resolution.iter() {
                    let resolved = lock
                        .content()
                        .get(addr)
                        .context(error::MalformedLockSnafu { addr: addr.clone() })?;
                    node.set_data(&resolved.data());
                }
                for (addr, node) in self.environments.iter() {
                    ctx.add_farm(addr, node).await?;
                }

                for (addr, node) in self.transforms.iter() {
                    ctx.add_transform(addr, node).await?;
                }
                return Ok(());
            } else if lock.digest() != digest && error_on_lock {
                return error::DependencyChangeSnafu {}.fail();
            }
        }

        // Resolve all storage backends
        for (addr, node) in self.source_caches.iter() {
            ctx.add_cache(addr, node).await?;
        }
        if let Some(node) = self.build_cache.as_ref() {
            ctx.add_cache(&Addr::parse("//edo-build-cache")?, node)
                .await?;
        }
        if let Some(node) = self.output_cache.as_ref() {
            ctx.add_cache(&Addr::parse("//edo-output-cache")?, node)
                .await?;
        }

        // Vendor's are only used during project resolution
        // Now we should create a resolver
        let mut resolver = Resolver::default();
        let mut vendors = HashMap::new();
        // Register all our vendors
        for (addr, node) in self.vendors.iter() {
            let vendor = ctx.add_vendor(addr, node).await?;
            vendors.insert(addr.to_string(), vendor.clone());
            debug!(
                section = "context",
                component = "project",
                "register vendor {addr}"
            );
            resolver.add_vendor(&addr.to_string(), vendor.clone());
        }

        // Now for every node needing resolution we need to get the vendor field to resolve
        let mut need_resolution = Vec::new();
        let mut assigners = HashMap::new();
        for (addr, node) in self.need_resolution.iter() {
            debug!(
                section = "context",
                component = "project",
                "{addr} needs resolution"
            );
            let dep = Dependency::from_node(addr, node, ctx).await?;
            assigners.insert(dep.addr.clone(), node.clone());
            // Populate the resolver for this dependency
            resolver.build_db(dep.name.as_str()).await?;
            need_resolution.push(dep);
        }

        // Now that we have built the databases we want to run the resolution
        // unfortunately due to resolvo using its own async through rayno hidden behind only
        // synchronous calls we have to use spawn_blocking here
        let resolved = tokio::task::spawn_blocking(move || resolver.resolve(need_resolution))
            .await
            .unwrap()?;

        // Create the new lock
        let mut lock = Lock::new(digest);

        for (addr, (vendor_name, name, version)) in resolved.iter() {
            debug!(
                section = "context",
                component = "project",
                "resolved {addr} to {name}@{version} from vendor {vendor_name}"
            );
            let vendor = vendors.get(vendor_name).unwrap();
            let target = assigners.get(addr).unwrap();
            let resolved = vendor.resolve(name, version).await?;
            lock.content_mut().insert(addr.clone(), resolved.clone());
            target.set_data(&resolved.data());
        }

        for (addr, node) in self.environments.iter() {
            debug!(
                section = "context",
                component = "project",
                "adding environment farm {addr}"
            );
            ctx.add_farm(addr, node).await?;
        }

        for (addr, node) in self.transforms.iter() {
            debug!(
                section = "context",
                component = "project",
                "adding transform {addr}"
            );
            ctx.add_transform(addr, node).await?;
        }

        // Write out the lock file
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(self.project_path.join("edo.lock.json"))
            .context(error::IoSnafu)?;

        serde_json::to_writer_pretty(&mut file, &lock).context(error::SerializeSnafu)?;
        Ok(())
    }
}

/// Implements [`Definable`](crate::context::Definable) as a no-op for types that require no configuration.
#[macro_export]
macro_rules! non_configurable {
    ($ty: ident, $e: ty) => {
        impl $crate::context::Definable<$e, $crate::context::NonConfigurable<$e>> for $ty {
            fn key() -> &'static str {
                "noop"
            }

            fn set_config(
                &mut self,
                _: &$crate::context::NonConfigurable<$e>,
            ) -> std::result::Result<(), $e> {
                Ok(())
            }
        }
    };
}

/// Implements [`DefinableNoContext`](crate::context::DefinableNoContext) as a no-op for types that require no configuration.
#[macro_export]
macro_rules! non_configurable_no_context {
    ($ty: ident, $e: ty) => {
        impl $crate::context::DefinableNoContext<$e, $crate::context::NonConfigurable<$e>> for $ty {
            fn key() -> &'static str {
                "noop"
            }

            fn set_config(
                &mut self,
                _: &$crate::context::NonConfigurable<$e>,
            ) -> std::result::Result<(), $e> {
                Ok(())
            }
        }
    };
}

pub use non_configurable;
pub use non_configurable_no_context;
