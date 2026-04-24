use std::collections::{BTreeMap, HashMap};
use std::fs::{File, read, read_dir};
use std::path::{Path, PathBuf};

use super::Context;
use super::address::Addr;
use super::lock::Lock;
use super::starlark::{Store, starlark_bindings};
use super::{ContextResult as Result, FromNode, Node, error};
use crate::context::schema::Schema;
use crate::source::{Dependency, Resolver};
use snafu::{OptionExt, ResultExt};
use starlark::environment::{GlobalsBuilder, Module};
use starlark::eval::Evaluator;
use starlark::syntax::{AstModule, Dialect};

pub struct Project {
    project_path: PathBuf,
    backends: BTreeMap<Addr, Node>,
    vendors: BTreeMap<Addr, Node>,
    plugins: BTreeMap<Addr, Node>,
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
            table.insert("source".into(), sources.get(&caddr).context(error::NotValidSourceSnafu {
                id: addr
            })?.clone());
        } else if let Some(list) = src.as_list() {
            let mut items = Vec::new();
            for item in list.iter() {
                let addr = item.as_string().context(error::FieldSnafu {
                    field: "requires",
                    type_: "array of string / string"
                })?;
                let caddr = if addr.starts_with("//") {
                    Addr::parse(&addr)?
                } else {
                    namespace.join(&addr)
                };
                items.push(sources.get(&caddr).context(error::NotValidSourceSnafu {
                    id: addr
                })?.clone());
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

    pub async fn load<P: AsRef<Path>>(path: P, ctx: &Context, error_on_lock: bool) -> Result<()> {
        let mut project = Self {
            project_path: path.as_ref().to_path_buf(),
            backends: BTreeMap::new(),
            vendors: BTreeMap::new(),
            plugins: BTreeMap::new(),
            environments: BTreeMap::new(),
            transforms: BTreeMap::new(),
            need_resolution: BTreeMap::new(),
        };

        project.walk(&Addr::default(), path.as_ref())?;
        project.build(ctx, error_on_lock).await?;
        Ok(())
    }

    fn walk(&mut self, namespace: &Addr, directory: &Path) -> Result<()> {
        let read = read_dir(directory).context(error::IoSnafu)?;
        for entry in read {
            let entry = entry.context(error::IoSnafu)?;
            let path = entry.path();
            if path.is_file()
                && path
                    .file_name()
                    .and_then(|x| x.to_str())
                    .unwrap()
                   == "edo.toml"
            {
                // This is a barkml defined build file
                self.load_toml(namespace, &path)?;
            } else if path.is_file() && path.extension().and_then(|x| x.to_str()) == Some("edo") {
                // This is a starlark build file
                self.load_starlark(namespace, &path)?;
            } else if path.is_dir() {
                let dir_name = path.file_name().and_then(|x| x.to_str()).unwrap();
                let addr = namespace.join(dir_name);
                self.walk(&addr, &path)?;
            }
        }
        Ok(())
    }

    fn load_starlark(&mut self, namespace: &Addr, file: &Path) -> Result<()> {
        let code = std::fs::read_to_string(file).context(error::IoSnafu)?;
        let ast = AstModule::parse(file.to_str().unwrap(), code, &Dialect::Standard)?;
        let globals = GlobalsBuilder::standard().with(starlark_bindings).build();
        let module = Module::new();
        let store = Store::default();
        let mut eval = Evaluator::new(&module);
        eval.extra = Some(&store);
        eval.eval_module(ast, &globals)?;
        for node in store.nodes().values() {
            if let Some(id) = node.get_id() {
                match id.as_str() {
                    "source_cache" => {
                        let addr = namespace.join(node.get_name().unwrap().as_str());
                        self.backends.insert(addr, node.clone());
                    }
                    "output_cache" => {
                        self.backends
                            .insert(Addr::parse("//edo-output-cache").unwrap(), node.clone());
                    }
                    "build_cache" => {
                        // The build cache should always be at root address //edo-build-cache
                        self.backends
                            .insert(Addr::parse("//edo-build-cache").unwrap(), node.clone());
                    }
                    "vendor" => {
                        let addr = namespace.join(node.get_name().unwrap().as_str());
                        self.vendors.insert(addr, node.clone());
                    }
                    "environment" => {
                        let addr = namespace.join(node.get_name().unwrap().as_str());
                        self.environments.insert(addr.clone(), node.clone());
                        if let Some(node) = node.get("source") {
                            for entry in node
                                .as_list()
                                .unwrap_or(vec![node])
                                .iter()
                                .filter(|x| x.get_id() == Some("requires".to_string()))
                            {
                                let caddr = addr.join(entry.get_name().unwrap().as_str());
                                self.need_resolution.insert(caddr, entry.clone());
                            }
                        }
                    }
                    "transform" => {
                        let addr = namespace.join(node.get_name().unwrap().as_str());
                        self.transforms.insert(addr.clone(), node.clone());
                        if let Some(node) = node.get("source") {
                            for entry in node
                                .as_list()
                                .unwrap_or(vec![node])
                                .iter()
                                .filter(|x| x.get_id() == Some("requires".to_string()))
                            {
                                let caddr = addr.join(entry.get_name().unwrap().as_str());
                                self.need_resolution.insert(caddr, entry.clone());
                            }
                        }
                    }
                    "plugin" => {
                        let addr = namespace.join(node.get_name().unwrap().as_str());
                        self.plugins.insert(addr.clone(), node.clone());
                        if let Some(node) = node.get("source") {
                            for entry in node
                                .as_list()
                                .unwrap_or(vec![node])
                                .iter()
                                .filter(|x| x.get_id() == Some("requires".to_string()))
                            {
                                let caddr = addr.join(entry.get_name().unwrap().as_str());
                                self.need_resolution.insert(caddr, entry.clone());
                            }
                        }
                    }
                    _ => {
                        continue;
                    }
                }
            }
        }
        Ok(())
    }

    fn load_toml(&mut self, namespace: &Addr, file: &Path) -> Result<()> {
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
                for (name, node) in config.get_backend()? {
                    let addr = namespace.join(&name);
                    self.backends.insert(addr, node);
                }
                for (name, node) in config.get_plugins()? {
                    let addr = namespace.join(&name);
                    let cnode = handle_sources(namespace, &node, &sources)?;
                    self.plugins.insert(addr, cnode);
                }
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
            }
        }
        Ok(())
    }

    pub async fn build(&mut self, ctx: &Context, error_on_lock: bool) -> Result<()> {
        // Calculate the digest of the project configuration
        let digest = self.calculate_digest()?;
        // Check for an existing lockfile
        let lock_file = self.project_path.join("edo.lock.json");
        if lock_file.exists() {
            let mut file = File::open(&lock_file).context(error::IoSnafu)?;
            let lock: Lock = serde_json::from_reader(&mut file).context(error::SerializeSnafu)?;
            // Now check if the digests match, if so then we should use the lockfile to resolve our unresolved nodes
            if lock.digest == digest {
                info!(target: "project", "no changes detected in project, reusing lock resolution file");
                for (addr, node) in self.need_resolution.iter() {
                    let resolved = lock
                        .content
                        .get(addr)
                        .context(error::MalformedLockSnafu { addr: addr.clone() })?;
                    node.set_data(&resolved.data());
                }
                // Resolve all plugins
                for (addr, node) in self.plugins.iter() {
                    ctx.add_plugin(addr, node).await?;
                }
                // Resolve all storage backends
                for (addr, node) in self.backends.iter() {
                    ctx.add_cache(addr, node).await?;
                }
                for (addr, node) in self.environments.iter() {
                    ctx.add_farm(addr, node).await?;
                }

                for (addr, node) in self.transforms.iter() {
                    ctx.add_transform(addr, node).await?;
                }
                return Ok(());
            } else if lock.digest != digest && error_on_lock {
                return error::DependencyChangeSnafu {}.fail();
            }
        }

        // Plugins cannot have vendored sources as they need to be resolved first
        for (addr, node) in self.plugins.iter() {
            debug!(
                section = "context",
                component = "project",
                "adding plugin {addr}"
            );
            ctx.add_plugin(addr, node).await?;
        }

        // Resolve all storage backends
        for (addr, node) in self.backends.iter() {
            ctx.add_cache(addr, node).await?;
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
        let mut lock = Lock {
            digest,
            ..Default::default()
        };

        for (addr, (vendor_name, name, version)) in resolved.iter() {
            debug!(
                section = "context",
                component = "project",
                "resolved {addr} to {name}@{version} from vendor {vendor_name}"
            );
            let vendor = vendors.get(vendor_name).unwrap();
            let target = assigners.get(addr).unwrap();
            let resolved = vendor.resolve(name, version).await?;
            lock.content.insert(addr.clone(), resolved.clone());
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
