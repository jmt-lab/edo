//! TOML schema deserialization for `edo.toml` project files.
//!
//! [`Schema`] is the top-level enum dispatching on `schema-version`.
//! [`SchemaV1`] holds the v1 layout: config, cache, plugins, environments,
//! sources, transforms, vendors, and requires sections. [`Cache`] groups the
//! three cache categories (source, build, output). The [`toml_def_item`]
//! helper converts a raw TOML table entry into a [`Node`] definition.

use crate::context::{ContextResult, Node, error};
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use std::collections::BTreeMap;
use toml::map::Map;

/// Top-level schema envelope, dispatching on the `schema-version` field.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "schema-version")]
pub enum Schema {
    /// Version 1 of the edo project schema.
    #[serde(rename = "1")]
    V1(SchemaV1),
}

/// Groups the three cache categories in a v1 schema.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Cache {
    #[serde(default)]
    source: BTreeMap<String, toml::Value>,
    #[serde(default)]
    build: Option<toml::Value>,
    #[serde(default)]
    output: Option<toml::Value>,
}

/// Version 1 of the edo project schema, holding all configuration sections.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchemaV1 {
    #[serde(default)]
    config: Map<String, toml::Value>,
    #[serde(default)]
    cache: Cache,
    #[serde(default)]
    environment: BTreeMap<String, toml::Value>,
    #[serde(default)]
    source: BTreeMap<String, toml::Value>,
    #[serde(default)]
    transform: BTreeMap<String, toml::Value>,
    #[serde(default)]
    vendor: BTreeMap<String, toml::Value>,
    #[serde(default)]
    requires: BTreeMap<String, toml::Value>,
}

fn toml_map(table: &toml::map::Map<String, toml::Value>) -> ContextResult<BTreeMap<String, Node>> {
    let mut tree = BTreeMap::new();
    for (key, value) in table.iter() {
        tree.insert(key.clone(), Node::try_from(value)?);
    }
    Ok(tree)
}

/// Converts a single TOML table entry into a [`Node`] definition, extracting
/// the `kind` field and wrapping the remainder as the node's table.
fn toml_def_item(id: &str, name: &str, inner: &Map<String, toml::Value>) -> ContextResult<Node> {
    let mut shape = inner.clone();
    let kind = shape
        .remove("kind")
        .and_then(|x| x.as_str().map(|x| x.to_string()))
        .context(error::FieldSnafu {
            field: "kind",
            type_: "string",
        })?;
    let node_table = toml_map(&shape)?;
    Ok(Node::new_definition(id, &kind, name, node_table))
}

fn toml_def(
    table: &BTreeMap<String, toml::Value>,
    id: &str,
) -> ContextResult<BTreeMap<String, Node>> {
    let mut tree = BTreeMap::new();
    for (name, config) in table.iter() {
        if let Some(inner) = config.as_table() {
            tree.insert(name.clone(), toml_def_item(id, name, inner)?);
        }
    }
    Ok(tree)
}

impl SchemaV1 {
    /// Returns the source cache definitions as nodes.
    pub fn get_source_caches(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.cache.source, "backend")
    }

    /// Returns the build cache definition, if configured.
    pub fn get_build_cache(&self) -> ContextResult<Option<Node>> {
        if let Some(data) = self.cache.build.as_ref() {
            let table = data.as_table().context(error::FieldSnafu {
                field: "build_cache",
                type_: "table",
            })?;
            Ok(Some(toml_def_item("backend", "build_cache", table)?))
        } else {
            Ok(None)
        }
    }

    /// Returns the output cache definition, if configured.
    pub fn get_output_cache(&self) -> ContextResult<Option<Node>> {
        if let Some(data) = self.cache.output.as_ref() {
            let table = data.as_table().context(error::FieldSnafu {
                field: "output_cache",
                type_: "table",
            })?;
            Ok(Some(toml_def_item("backend", "output_cache", table)?))
        } else {
            Ok(None)
        }
    }

    /// Returns the user-level config entries as nodes.
    pub fn get_config(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_map(&self.config)
    }

    /// Returns the environment definitions as nodes.
    pub fn get_environments(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.environment, "environment")
    }

    /// Returns the source definitions as nodes.
    pub fn get_sources(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.source, "source")
    }

    /// Returns the transform definitions as nodes.
    pub fn get_transforms(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.transform, "transform")
    }

    /// Returns the vendor definitions as nodes.
    pub fn get_vendors(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.vendor, "vendor")
    }

    /// Returns the dependency requirement definitions as nodes.
    pub fn get_requires(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.requires, "requires")
    }
}

#[cfg(test)]
mod test {
    use crate::context::schema::Schema;

    #[test]
    fn test_deserialize() {
        let toml_str = r#"
schema-version = "1"
[vendor.public-ecr]
kind = "image"
uri  = "public.ecr.aws/docker/library"

[want.gcc]
kind = "image"
at   = "=14.3.0"

[environment.gcc]
kind   = "container"
source = "//hello_oci/gcc"

[source.code]
kind       = "local"
path       = "hello_oci"
out        = "."
is_archive = false

[transform.build]
kind        = "script"
environment = "//hello_oci/gcc"
source      = "//hello_oci/code"
commands    = [
    "mkdir -p {{install-root}}/bin",
    "gcc -o hello_oci hello.c",
    "cp hello_oci {{install-root}}/bin/hello_oci"
]"#;
        let _: Schema = toml::from_str(toml_str).expect("failed to parse");
    }
}
