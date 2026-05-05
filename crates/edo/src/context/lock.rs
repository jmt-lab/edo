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

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(s: &str) -> Addr {
        Addr::parse(s).unwrap()
    }

    #[test]
    fn new_has_correct_digest_and_empty_content() {
        let lock = Lock::new("digest".to_string());
        assert_eq!(lock.digest(), "digest");
        assert!(lock.content().is_empty());
    }

    #[test]
    fn content_mut_insert_is_reflected_in_content() {
        let mut lock = Lock::new("abc".to_string());
        let a = addr("//proj/node");
        let node = Node::new_string("value".to_string());
        lock.content_mut().insert(a.clone(), node);
        assert_eq!(lock.content().len(), 1);
        assert!(lock.content().contains_key(&a));
    }

    #[test]
    fn serde_json_round_trip() {
        let mut lock = Lock::new("round-trip".to_string());
        let a = addr("//proj/item");
        lock.content_mut()
            .insert(a.clone(), Node::new_string("value".to_string()));

        let json = serde_json::to_string(&lock).unwrap();
        let restored: Lock = serde_json::from_str(&json).unwrap();

        assert_eq!(restored.digest(), "round-trip");
        assert!(restored.content().contains_key(&a));
    }

    #[test]
    fn serde_rename_refs_not_content() {
        let mut lock = Lock::new("x".to_string());
        lock.content_mut()
            .insert(addr("//p/q"), Node::new_string("v".to_string()));

        let json = serde_json::to_string(&lock).unwrap();
        assert!(json.contains("\"refs\":"), "expected \"refs:\" in {json}");
        assert!(
            !json.contains("\"content\":"),
            "unexpected \"content:\" in {json}"
        );
    }
}
