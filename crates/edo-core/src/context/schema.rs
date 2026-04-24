use std::collections::BTreeMap;

use serde::{Serialize, Deserialize};
use snafu::OptionExt;
use toml::map::Map;

use crate::context::{ContextResult, Node, error};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "schema-version")]
pub enum Schema {
    #[serde(rename = "1")]
    V1(SchemaV1),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchemaV1 {
    #[serde(default)]
    pub config: Map<String, toml::Value>,
    #[serde(default)]
    pub backend: BTreeMap<String, toml::Value>,
    #[serde(default)]
    pub plugin: BTreeMap<String, toml::Value>,
    #[serde(default)]
    pub environment: BTreeMap<String, toml::Value>,
    #[serde(default)]
    pub source: BTreeMap<String, toml::Value>,
    #[serde(default)]
    pub transform: BTreeMap<String, toml::Value>,
    #[serde(default)]
    pub vendor: BTreeMap<String, toml::Value>,
    #[serde(default)]
    pub requires: BTreeMap<String, toml::Value>,
}

fn toml_map(table: &toml::map::Map<String, toml::Value>) -> ContextResult<BTreeMap<String, Node>> {
    let mut tree = BTreeMap::new();
    for (key, value) in table.iter() {
        tree.insert(key.clone(), Node::try_from(value)?);
    }
    Ok(tree)
}

fn toml_def(table: &BTreeMap<String, toml::Value>, id: &str) -> ContextResult<BTreeMap<String, Node>> {
    let mut tree = BTreeMap::new();
    for (name, config) in table.iter() {
        if let Some(inner) = config.as_table() {
            let mut shape = inner.clone();
            let kind = shape.remove("kind")
                .and_then(|x| x.as_str().map(|x| x.to_string()))
                .context(error::FieldSnafu {
                field: "kind",
                type_: "string"
            })?;
            let node_table = toml_map(&shape)?;
            tree.insert(name.clone(), Node::new_definition(id, &kind, name, node_table));
        }
    }
    Ok(tree)
}

impl SchemaV1 {
    pub fn get_backend(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.backend, "storage")
    }

    pub fn get_config(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_map(&self.config)
    }

    pub fn get_plugins(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.plugin, "plugin")
    }

    pub fn get_environments(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.environment, "environment")
    }

    pub fn get_sources(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.source, "source")
    }

    pub fn get_transforms(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.transform, "transform")
    }

    pub fn get_vendors(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.vendor, "vendor")
    }

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
