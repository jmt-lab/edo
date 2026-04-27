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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Cache {
    #[serde(default)]
    pub source: BTreeMap<String, toml::Value>,
    #[serde(default)]
    pub build: Option<toml::Value>,
    #[serde(default)]
    pub output: Option<toml::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SchemaV1 {
    #[serde(default)]
    pub config: Map<String, toml::Value>,
    #[serde(default)]
    pub cache: Cache,
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

fn toml_def_item(id: &str, name: &str, inner: &Map<String, toml::Value>) -> ContextResult<Node> {
    let mut shape = inner.clone();
    let kind = shape.remove("kind")
        .and_then(|x| x.as_str().map(|x| x.to_string()))
        .context(error::FieldSnafu {
        field: "kind",
        type_: "string"
    })?;
    let node_table = toml_map(&shape)?;
    Ok(Node::new_definition(id, &kind, name, node_table))
}

fn toml_def(table: &BTreeMap<String, toml::Value>, id: &str) -> ContextResult<BTreeMap<String, Node>> {
    let mut tree = BTreeMap::new();
    for (name, config) in table.iter() {
        if let Some(inner) = config.as_table() {
            tree.insert(name.clone(), toml_def_item(id, name, inner)?);
        }
    }
    Ok(tree)
}

impl SchemaV1 {
    pub fn get_source_caches(&self) -> ContextResult<BTreeMap<String, Node>> {
        toml_def(&self.cache.source, "backend")
    }

    pub fn get_build_cache(&self) -> ContextResult<Option<Node>> {
        if let Some(data) = self.cache.build.as_ref() {
            let table = data.as_table().context(error::FieldSnafu {
                field: "build_cache",
                type_: "table"
            })?;
            Ok(Some(toml_def_item("backend", "build_cache", table)?))
        } else {
            Ok(None)
        }
    }

    pub fn get_output_cache(&self) -> ContextResult<Option<Node>> {
        if let Some(data) = self.cache.output.as_ref() {
            let table = data.as_table().context(error::FieldSnafu {
                field: "output_cache",
                type_: "table"
            })?;
            Ok(Some(toml_def_item("backend", "output_cache", table)?))
        } else {
            Ok(None)
        }
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
