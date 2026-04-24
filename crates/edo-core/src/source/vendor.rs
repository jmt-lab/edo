use super::SourceResult;
use crate::context::Node;
use arc_handle::arc_handle;
use async_trait::async_trait;
use semver::{Version, VersionReq};
use std::collections::{HashMap, HashSet};

/// A Vendor represents a remote provider for sources with support for dependency resolution
#[arc_handle]
#[async_trait]
pub trait Vendor {
    /// Get all versions of a given package/source name
    async fn get_options(&self, name: &str) -> SourceResult<HashSet<Version>>;
    /// Resolve a given name and version into a valid source node
    async fn resolve(&self, name: &str, version: &Version) -> SourceResult<Node>;
    /// Get all dependency requirements for a given namme and version
    async fn get_dependencies(
        &self,
        name: &str,
        version: &Version,
    ) -> SourceResult<Option<HashMap<String, VersionReq>>>;
}
