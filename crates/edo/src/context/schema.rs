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
    #[allow(dead_code)]
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
    use crate::context::ContextError;
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

    #[test]
    fn deserialize_empty_v1_defaults_all_fields() {
        let s: Schema = toml::from_str("schema-version = \"1\"").expect("parse");
        let Schema::V1(v1) = s;
        assert!(v1.get_source_caches().unwrap().is_empty());
        assert!(v1.get_build_cache().unwrap().is_none());
        assert!(v1.get_output_cache().unwrap().is_none());
        assert!(v1.get_environments().unwrap().is_empty());
        assert!(v1.get_sources().unwrap().is_empty());
        assert!(v1.get_transforms().unwrap().is_empty());
        assert!(v1.get_vendors().unwrap().is_empty());
        assert!(v1.get_requires().unwrap().is_empty());
    }

    #[test]
    fn unknown_schema_version_fails() {
        let r: Result<Schema, _> = toml::from_str("schema-version = \"99\"");
        assert!(r.is_err());
    }

    #[test]
    fn get_sources_produces_definition_with_id_source_and_kind() {
        let toml_str = r#"schema-version = "1"
[source.foo]
kind = "local"
path = "x"
"#;
        let Schema::V1(v1) = toml::from_str(toml_str).unwrap();
        let sources = v1.get_sources().unwrap();
        let node = sources.get("foo").expect("foo exists");
        assert_eq!(node.get_id().as_deref(), Some("source"));
        assert_eq!(node.get_kind().as_deref(), Some("local"));
        assert_eq!(node.get_name().as_deref(), Some("foo"));
        // the `kind` key is stripped from the table; `path` remains
        let table = node.get_table().unwrap();
        assert!(table.contains_key("path"));
        assert!(!table.contains_key("kind"));
    }

    #[test]
    fn category_ids_match_section_names() {
        let toml_str = r#"schema-version = "1"
[environment.e]
kind = "container"
[transform.t]
kind = "script"
[vendor.v]
kind = "image"
[requires.r]
kind = "image"
at = "=1.0.0"
[cache.source.c]
kind = "local"
path = "/tmp"
"#;
        let Schema::V1(v1) = toml::from_str(toml_str).unwrap();
        assert_eq!(
            v1.get_environments()
                .unwrap()
                .get("e")
                .unwrap()
                .get_id()
                .as_deref(),
            Some("environment")
        );
        assert_eq!(
            v1.get_transforms()
                .unwrap()
                .get("t")
                .unwrap()
                .get_id()
                .as_deref(),
            Some("transform")
        );
        assert_eq!(
            v1.get_vendors()
                .unwrap()
                .get("v")
                .unwrap()
                .get_id()
                .as_deref(),
            Some("vendor")
        );
        assert_eq!(
            v1.get_requires()
                .unwrap()
                .get("r")
                .unwrap()
                .get_id()
                .as_deref(),
            Some("requires")
        );
        assert_eq!(
            v1.get_source_caches()
                .unwrap()
                .get("c")
                .unwrap()
                .get_id()
                .as_deref(),
            Some("backend")
        );
    }

    #[test]
    fn build_and_output_cache_present() {
        let toml_str = r#"schema-version = "1"
[cache.build]
kind = "local"
path = "/tmp/b"
[cache.output]
kind = "local"
path = "/tmp/o"
"#;
        let Schema::V1(v1) = toml::from_str(toml_str).unwrap();
        let b = v1.get_build_cache().unwrap().expect("build");
        assert_eq!(b.get_kind().as_deref(), Some("local"));
        let o = v1.get_output_cache().unwrap().expect("output");
        assert_eq!(o.get_kind().as_deref(), Some("local"));
    }

    #[test]
    fn build_cache_non_table_returns_field_error() {
        let toml_str = r#"schema-version = "1"
[cache]
build = "not a table"
"#;
        let Schema::V1(v1) = toml::from_str(toml_str).unwrap();
        let err = v1.get_build_cache().expect_err("expected Field error");
        assert!(
            matches!(err, ContextError::Field { ref field, .. } if field == "build_cache"),
            "got: {err:?}"
        );
    }

    #[test]
    fn output_cache_non_table_returns_field_error() {
        let toml_str = r#"schema-version = "1"
[cache]
output = "not a table"
"#;
        let Schema::V1(v1) = toml::from_str(toml_str).unwrap();
        let err = v1.get_output_cache().expect_err("expected Field error");
        assert!(
            matches!(err, ContextError::Field { ref field, .. } if field == "output_cache"),
            "got: {err:?}"
        );
    }

    #[test]
    fn missing_kind_in_transform_returns_field_error() {
        let toml_str = r#"schema-version = "1"
[transform.build]
source = "x"
"#;
        let Schema::V1(v1) = toml::from_str(toml_str).unwrap();
        let err = v1.get_transforms().expect_err("expected Field error");
        assert!(
            matches!(err, ContextError::Field { ref field, .. } if field == "kind"),
            "got: {err:?}"
        );
    }
}
