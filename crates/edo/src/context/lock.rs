//! Dependency lock file representation.
//!
//! A [`Lock`] captures the resolved dependency graph (digest + content map)
//! so that subsequent builds can skip resolution when the project
//! configuration has not changed. It is serialized as `edo.lock.json`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::Node;

use super::Addr;

/// A serializable lock file that records the digest of the project
/// configuration and the resolved dependency nodes.
#[derive(Default, Serialize, Deserialize)]
pub struct Lock {
    digest: String,
    #[serde(rename = "refs")]
    content: BTreeMap<Addr, Node>,
}

impl Lock {
    /// Creates a new `Lock` with the given digest and empty content.
    pub fn new(digest: String) -> Self {
        Self {
            digest,
            content: BTreeMap::new(),
        }
    }

    /// Returns the digest string.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Returns a reference to the content map.
    pub fn content(&self) -> &BTreeMap<Addr, Node> {
        &self.content
    }

    /// Returns a mutable reference to the content map.
    pub fn content_mut(&mut self) -> &mut BTreeMap<Addr, Node> {
        &mut self.content
    }
}
