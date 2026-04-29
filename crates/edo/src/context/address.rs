//! Hierarchical addressing for build graph nodes.
//!
//! Every artifact, transform, environment, and plugin in an edo project is
//! identified by an [`Addr`] — a slash-separated path such as `//project/build`.
//! The [`Addressable`] trait provides a uniform way to query the address,
//! name, and kind of any addressable entity.

use super::ContextResult as Result;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A trait for entities that can be identified by an [`Addr`].
pub trait Addressable {
    /// Returns the address of this entity.
    fn addr(&self) -> &Addr;
    /// Returns the human-readable name of this entity.
    fn name(&self) -> &String;
    /// Returns the kind identifier (e.g. `"local"`, `"container"`).
    fn kind(&self) -> &String;
}

/// A hierarchical, slash-separated address used to uniquely identify nodes in the build graph.
///
/// Addresses are serialized with a `//` prefix (e.g. `//project/build`) and
/// ordered lexicographically by their segments.
#[derive(Clone, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Addr(Vec<String>);

impl<'de> Deserialize<'de> for Addr {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Addr::parse(s.as_str()).map_err(serde::de::Error::custom)
    }
}

impl Serialize for Addr {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = self.to_string();
        serializer.serialize_str(s.as_str())
    }
}

impl Addr {
    /// Parses a slash-separated address string into an `Addr`.
    ///
    /// An optional `//` prefix is stripped before splitting on `/`.
    pub fn parse(input: &str) -> Result<Self> {
        let segment = input.strip_prefix("//").unwrap_or(input);
        Ok(Self(segment.split("/").map(|x| x.to_string()).collect()))
    }

    /// Creates a child address by appending `name` as a new segment.
    pub fn join(&self, name: &str) -> Self {
        let mut content = self.0.clone();
        content.push(name.to_string());
        Self(content)
    }

    /// Returns the parent address by removing the last segment, or `None` if this is a root-level address.
    pub fn parent(&self) -> Option<Addr> {
        if self.0.len() == 1 {
            None
        } else {
            let mut me = self.0.clone();
            me.pop();
            Some(Addr(me))
        }
    }

    /// Returns the address segments joined by `/` without the leading `//` prefix.
    pub fn to_id(&self) -> String {
        self.0.join("/")
    }
}

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("//{}", self.0.join("/")))
    }
}
