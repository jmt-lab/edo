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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    fn addr(s: &str) -> Addr {
        Addr::parse(s).expect("parse addr")
    }

    #[test]
    fn parse_with_double_slash_prefix() {
        let a = addr("//a/b/c");
        assert_eq!(a.to_string(), "//a/b/c");
        assert_eq!(a.to_id(), "a/b/c");
    }

    #[test]
    fn parse_without_prefix() {
        let a = addr("a/b");
        assert_eq!(a.to_string(), "//a/b");
    }

    #[test]
    fn parse_single_segment() {
        let a = addr("x");
        assert_eq!(a.to_string(), "//x");
        assert_eq!(a.parent(), None);
    }

    #[test]
    fn parse_empty_string() {
        // An empty string splits into a single empty segment.
        let a = addr("");
        assert_eq!(a.to_string(), "//");
        assert_eq!(a.parent(), None);
    }

    #[test]
    fn parse_trailing_slash_yields_empty_segment() {
        let a = addr("//a/");
        // `"a/".split('/')` produces ["a", ""], so a trailing empty segment is preserved.
        assert_eq!(a.to_string(), "//a/");
    }

    #[test]
    fn default_round_trips_through_display() {
        let a = Addr::default();
        // Default is an empty Vec → Display emits "//" (empty join).
        assert_eq!(a.to_string(), "//");
        assert_eq!(a.to_id(), "");
    }

    #[test]
    fn join_appends_segment() {
        let a = addr("//a/b").join("c");
        assert_eq!(a.to_string(), "//a/b/c");
    }

    #[test]
    fn join_chained() {
        let a = addr("//root").join("child").join("grand");
        assert_eq!(a.to_string(), "//root/child/grand");
    }

    #[test]
    fn parent_drops_last_segment() {
        let a = addr("//a/b/c");
        assert_eq!(a.parent(), Some(addr("//a/b")));
    }

    #[test]
    fn parent_of_single_segment_is_none() {
        assert_eq!(addr("//only").parent(), None);
    }

    #[test]
    fn to_id_strips_double_slash_prefix() {
        let a = addr("//seg1/seg2");
        assert_eq!(a.to_string(), "//seg1/seg2");
        assert_eq!(a.to_id(), "seg1/seg2");
    }

    #[test]
    fn ordering_is_lexicographic_segmentwise() {
        let mut v = vec![addr("//b/a"), addr("//a/z"), addr("//a/a")];
        v.sort();
        assert_eq!(v, vec![addr("//a/a"), addr("//a/z"), addr("//b/a")]);
    }

    #[test]
    fn hash_and_eq_consistent() {
        let a = addr("//a/b");
        let b = addr("//a/b");
        assert_eq!(a, b);
        assert_eq!(a.clone(), a);
        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        a.hash(&mut h1);
        b.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn serde_json_roundtrip() {
        let a = addr("//a/b/c");
        let json = serde_json::to_string(&a).expect("serialize");
        assert_eq!(json, "\"//a/b/c\"");
        let back: Addr = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(a, back);
    }

    #[test]
    fn serde_toml_roundtrip() {
        #[derive(serde::Serialize, serde::Deserialize)]
        struct Wrap {
            a: Addr,
        }
        let wrap = Wrap { a: addr("//x/y") };
        let s = toml::to_string(&wrap).expect("ser");
        assert!(s.contains("a = \"//x/y\""), "unexpected toml: {s}");
        let back: Wrap = toml::from_str(&s).expect("de");
        assert_eq!(back.a, wrap.a);
    }

    #[test]
    fn deserialize_non_string_fails() {
        let r: std::result::Result<Addr, _> = serde_json::from_str("42");
        assert!(r.is_err(), "expected deserialization failure for number");
    }
}
