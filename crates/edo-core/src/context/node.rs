//! Generic data tree used throughout the build graph.
//!
//! [`Node`] is a thread-safe, reference-counted wrapper around [`Data`] that
//! represents any value in an edo configuration — scalars, lists, tables, and
//! full definitions (id + kind + name + table). [`Component`] enumerates the
//! high-level plugin component types. The [`FromNode`] and
//! [`FromNodeNoContext`] traits define how typed values are constructed from
//! raw nodes.

use std::{collections::BTreeMap, fmt, sync::Arc};

use super::{Addr, Config, ContextResult as Result, error};
use async_trait::async_trait;
use parking_lot::RwLock;
use semver::{Version, VersionReq};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use snafu::ensure;

/// Identifies the type of plugin component.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Component {
    /// A storage backend component.
    StorageBackend,
    /// A build environment component.
    Environment,
    /// A source fetcher component.
    Source,
    /// A build transform component.
    Transform,
    /// A dependency vendor component.
    Vendor,
}

impl fmt::Display for Component {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StorageBackend => f.write_str("storage-backend"),
            Self::Environment => f.write_str("environment"),
            Self::Source => f.write_str("source"),
            Self::Transform => f.write_str("transform"),
            Self::Vendor => f.write_str("vendor"),
        }
    }
}

/// A thread-safe, reference-counted configuration value.
///
/// Wraps a [`Data`] variant behind an `Arc<RwLock<…>>` so it can be shared
/// across async tasks and mutated when dependency resolution fills in
/// resolved data.
#[derive(Clone, Debug)]
pub struct Node {
    data: Arc<RwLock<Data>>,
}

impl Serialize for Node {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let data = self.data.read().clone();
        data.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Node {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let data = Data::deserialize(deserializer)?;
        Ok(Self {
            data: Arc::new(RwLock::new(data)),
        })
    }
}

/// The underlying data variants that a [`Node`] can hold.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Data {
    /// A boolean value.
    Bool(bool),
    /// A 64-bit signed integer.
    Int(i64),
    /// A 64-bit floating-point number.
    Float(f64),
    /// A UTF-8 string.
    String(String),
    /// A semver version requirement (e.g. `>=1.0`).
    Require(VersionReq),
    /// A semver version (e.g. `1.2.3`).
    Version(Version),
    /// An ordered list of child nodes.
    List(Vec<Node>),
    /// A named definition with id, kind, name, and a key-value table.
    Definition {
        /// Unique identifier for this definition.
        id: String,
        /// The kind discriminator (e.g. `"local"`, `"container"`).
        kind: String,
        /// Human-readable name.
        name: String,
        /// Additional key-value pairs.
        #[serde(flatten)]
        table: BTreeMap<String, Node>,
    },
    /// An anonymous key-value table.
    Table(BTreeMap<String, Node>),
}

/// Constructs a typed value from a [`Node`] using only a [`Config`] reference
/// (no full `Context` needed).
#[async_trait]
pub trait FromNodeNoContext: Sized {
    /// The error type returned when conversion fails.
    type Error;

    /// Builds `Self` from the given node and configuration.
    async fn from_node(
        addr: &Addr,
        node: &Node,
        config: &Config,
    ) -> std::result::Result<Self, Self::Error>;
}

/// Constructs a typed value from a [`Node`] using the full [`super::Context`].
#[async_trait]
pub trait FromNode: Sized {
    /// The error type returned when conversion fails.
    type Error;

    /// Builds `Self` from the given node and build context.
    async fn from_node(
        addr: &Addr,
        node: &Node,
        ctx: &super::Context,
    ) -> std::result::Result<Self, Self::Error>;
}

impl<'a> TryFrom<&'a JsonValue> for Node {
    type Error = error::ContextError;

    fn try_from(value: &'a JsonValue) -> std::result::Result<Self, Self::Error> {
        let data = Data::try_from(value)?;
        Ok(Self {
            data: Arc::new(RwLock::new(data)),
        })
    }
}

impl<'a> TryFrom<&'a JsonValue> for Data {
    type Error = error::ContextError;

    fn try_from(value: &'a JsonValue) -> std::result::Result<Self, Self::Error> {
        match value {
            JsonValue::Bool(flag) => Ok(Data::new_bool(*flag)),
            JsonValue::String(string) => Ok(Data::new_string(string.clone())),
            JsonValue::Number(number) => {
                if number.is_f64() {
                    Ok(Data::new_float(number.as_f64().unwrap()))
                } else {
                    Ok(Data::new_int(number.as_i64().unwrap()))
                }
            }
            JsonValue::Array(entries) => {
                let mut values = Vec::new();
                for entry in entries {
                    values.push(Node::try_from(entry)?);
                }
                Ok(Data::new_list(values))
            }
            JsonValue::Object(content) => {
                let mut values = BTreeMap::new();
                for (key, value) in content {
                    values.insert(key.clone(), Node::try_from(value)?);
                }
                Ok(Data::new_table(values))
            }
            _ => error::NodeSnafu {}.fail(),
        }
    }
}

impl<'a> TryFrom<&'a toml::Value> for Node {
    type Error = error::ContextError;

    fn try_from(value: &'a toml::Value) -> std::result::Result<Self, Self::Error> {
        let data = Data::try_from(value)?;
        Ok(Node {
            data: Arc::new(RwLock::new(data)),
        })
    }
}

impl<'a, 'v> TryFrom<&'a toml::Value> for Data {
    type Error = error::ContextError;

    fn try_from(value: &'a toml::Value) -> std::result::Result<Self, Self::Error> {
        if let Some(flag) = value.as_bool() {
            Ok(Self::new_bool(flag))
        } else if let Some(string) = value.as_str() {
            // If we have a string we need to try and fail some things
            if let Ok(require) = VersionReq::parse(string) {
                Ok(Self::new_require(require))
            } else if let Ok(version) = Version::parse(string) {
                Ok(Self::new_version(version))
            } else {
                Ok(Self::new_string(string.to_string()))
            }
        } else if let Some(float) = value.as_float() {
            Ok(Self::new_float(float))
        } else if let Some(int) = value.as_integer() {
            Ok(Self::new_int(int))
        } else if let Some(items) = value.as_array() {
            let mut array = Vec::new();
            for item in items.iter() {
                array.push(Node::try_from(item)?);
            }
            Ok(Self::new_list(array))
        } else if let Some(items) = value.as_table() {
            let mut table = BTreeMap::new();
            for (key, value) in items.iter() {
                table.insert(key.clone(), Node::try_from(value)?);
            }
            Ok(Self::new_table(table))
        } else {
            error::NodeSnafu {}.fail()
        }
    }
}

macro_rules! as_fn {
    ($fn0: ident, $fn1: ident, $type: ident, $rtype: ty, $doc_new: expr, $doc_as: expr) => {
        #[doc = $doc_new]
        pub fn $fn0(value: $rtype) -> Self {
            Self::$type(value)
        }

        #[doc = $doc_as]
        pub fn $fn1(&self) -> Option<&$rtype> {
            match self {
                Self::$type(value) => Some(value),
                _ => None,
            }
        }
    };
}

macro_rules! get_field {
    ($gfn: ident, $sfn: ident, $field: ident, $rtype: ty, $doc_get: expr, $doc_set: expr) => {
        #[doc = $doc_get]
        pub fn $gfn(&self) -> Option<&$rtype> {
            match self {
                Self::Definition { $field, .. } => Some($field),
                _ => None,
            }
        }

        #[doc = $doc_set]
        pub fn $sfn(&mut self, value: $rtype) {
            match self {
                Self::Definition { $field, .. } => {
                    *$field = value;
                }
                _ => {}
            }
        }
    };
}

impl Data {
    as_fn!(new_bool, as_bool, Bool, bool, "Creates a new boolean data value.", "Returns the boolean value if this is a `Bool` variant.");
    as_fn!(new_int, as_int, Int, i64, "Creates a new integer data value.", "Returns the integer value if this is an `Int` variant.");
    as_fn!(new_float, as_float, Float, f64, "Creates a new floating-point data value.", "Returns the float value if this is a `Float` variant.");
    as_fn!(new_string, as_string, String, String, "Creates a new string data value.", "Returns the string value if this is a `String` variant.");
    as_fn!(new_version, as_version, Version, Version, "Creates a new semver version data value.", "Returns the version if this is a `Version` variant.");
    as_fn!(new_require, as_require, Require, VersionReq, "Creates a new version requirement data value.", "Returns the version requirement if this is a `Require` variant.");
    as_fn!(new_list, as_list, List, Vec<Node>, "Creates a new list data value.", "Returns the list of nodes if this is a `List` variant.");
    as_fn!(new_table, as_table, Table, BTreeMap<String, Node>, "Creates a new table data value.", "Returns the table if this is a `Table` variant.");
    get_field!(get_id, set_id, id, String, "Returns the definition id if this is a `Definition` variant.", "Sets the definition id if this is a `Definition` variant.");
    get_field!(get_kind, set_kind, kind, String, "Returns the definition kind if this is a `Definition` variant.", "Sets the definition kind if this is a `Definition` variant.");
    get_field!(get_name, set_name, name, String, "Returns the definition name if this is a `Definition` variant.", "Sets the definition name if this is a `Definition` variant.");
    get_field!(get_table, set_table, table, BTreeMap<String, Node>, "Returns the definition table if this is a `Definition` variant.", "Sets the definition table if this is a `Definition` variant.");

    pub(crate) fn append(&mut self, item: Node) {
        if let Self::List(items) = self {
            items.push(item)
        }
    }
}

macro_rules! node_field {
    ($gfn: ident, $sfn: ident, $field: ident, $rtype: ty, $doc_get: expr, $doc_set: expr) => {
        #[doc = $doc_get]
        pub fn $gfn(&self) -> Option<$rtype> {
            self.data.read().$gfn().cloned()
        }

        #[doc = $doc_set]
        pub fn $sfn(&self, value: $rtype) {
            self.data.write().$sfn(value)
        }
    };
}

macro_rules! node_as {
    ($fn0: ident, $fn1: ident, $rtype: ty, $doc_new: expr, $doc_as: expr) => {
        #[doc = $doc_new]
        pub fn $fn0(value: $rtype) -> Self {
            Self {
                data: Arc::new(RwLock::new(Data::$fn0(value))),
            }
        }

        #[doc = $doc_as]
        pub fn $fn1(&self) -> Option<$rtype> {
            self.data.read().$fn1().cloned()
        }
    };
}

impl Node {
    node_as!(new_bool, as_bool, bool, "Creates a new node holding a boolean value.", "Returns the boolean value if the underlying data is a `Bool`.");
    node_as!(new_int, as_int, i64, "Creates a new node holding an integer value.", "Returns the integer value if the underlying data is an `Int`.");
    node_as!(new_float, as_float, f64, "Creates a new node holding a floating-point value.", "Returns the float value if the underlying data is a `Float`.");
    node_as!(new_string, as_string, String, "Creates a new node holding a string value.", "Returns the string value if the underlying data is a `String`.");
    node_as!(new_version, as_version, Version, "Creates a new node holding a semver version.", "Returns the version if the underlying data is a `Version`.");
    node_as!(new_require, as_require, VersionReq, "Creates a new node holding a version requirement.", "Returns the version requirement if the underlying data is a `Require`.");
    node_as!(new_list, as_list, Vec<Node>, "Creates a new node holding a list of child nodes.", "Returns the list of child nodes if the underlying data is a `List`.");
    node_as!(new_table, as_table, BTreeMap<String, Node>, "Creates a new node holding a key-value table.", "Returns the table if the underlying data is a `Table`.");
    node_field!(get_id, set_id, id, String, "Returns the definition id if the underlying data is a `Definition`.", "Sets the definition id if the underlying data is a `Definition`.");
    node_field!(get_kind, set_kind, kind, String, "Returns the definition kind if the underlying data is a `Definition`.", "Sets the definition kind if the underlying data is a `Definition`.");
    node_field!(get_name, set_name, name, String, "Returns the definition name if the underlying data is a `Definition`.", "Sets the definition name if the underlying data is a `Definition`.");
    node_field!(get_table, set_table, table, BTreeMap<String, Node>, "Returns the definition table if the underlying data is a `Definition`.", "Sets the definition table if the underlying data is a `Definition`.");

    /// Creates a new node holding a [`Data::Definition`] with the given id, kind, name, and table.
    pub fn new_definition(id: &str, kind: &str, name: &str, table: BTreeMap<String, Node>) -> Self {
        Self {
            data: Arc::new(RwLock::new(Data::Definition {
                id: id.into(),
                kind: kind.into(),
                name: name.into(),
                table,
            })),
        }
    }

    /// Checks that the node's table contains all of the specified keys, returning
    /// an error listing any that are missing.
    pub fn validate_keys(&self, keys: &[&str]) -> Result<()> {
        if let Some(table) = self.as_table().or(self.get_table()) {
            let mut missing = Vec::new();
            for key in keys {
                let key = key.to_string();
                if !table.contains_key(&key) {
                    missing.push(key);
                }
            }
            ensure!(
                missing.is_empty(),
                error::NodeMissingKeysSnafu { keys: missing }
            );
        }
        Ok(())
    }

    /// Returns a clone of the underlying [`Data`] value.
    pub fn data(&self) -> Data {
        self.data.read().clone()
    }

    /// Replaces the underlying [`Data`] value with a clone of the given data.
    pub fn set_data(&self, data: &Data) {
        *self.data.write() = data.clone();
    }

    pub(crate) fn append(&self, value: Node) {
        self.data.write().append(value);
    }

    /// Looks up a child node by key in the table or definition table.
    pub fn get(&self, key: &str) -> Option<Node> {
        let read_lock = self.data.read();
        if let Some(table) = read_lock.as_table().or(read_lock.get_table()) {
            table.get(key).cloned()
        } else {
            None
        }
    }
}
