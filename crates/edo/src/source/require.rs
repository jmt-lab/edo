use super::error;
use async_trait::async_trait;
use semver::VersionReq;
use snafu::{OptionExt, ensure};

use crate::context::{Addr, Context, FromNode, Node};

/// A resolved dependency requirement parsed from an `edo.toml` `requires` node.
///
/// Contains the package name, its semver version requirement, the optional
/// vendor hint, and the originating address in the project graph.
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Dependency {
    /// The address of the node that declared this dependency.
    pub addr: Addr,
    /// The source kind (e.g. `"image"`, `"git"`).
    pub kind: String,
    /// The package name being depended upon.
    pub name: String,
    /// The semver requirement that must be satisfied.
    pub version: VersionReq,
    /// Optional vendor name constraining which registry to resolve from.
    pub vendor: Option<String>,
}

#[async_trait]
impl FromNode for Dependency {
    type Error = super::error::SourceError;

    async fn from_node(
        addr: &Addr,
        node: &Node,
        _ctx: &Context,
    ) -> std::result::Result<Self, Self::Error> {
        let id = node.get_id().context(error::UndefinedSnafu)?;
        ensure!(id == "requires", error::UndefinedSnafu {});
        node.validate_keys(&["at"])?;
        let kind = node.get_kind().context(error::UndefinedSnafu)?;
        let name = node.get_name().context(error::UndefinedSnafu)?;
        let version = node
            .get("at")
            .context(error::NoRequireSnafu)?
            .as_require()
            .context(error::FieldSnafu {
                field: "at",
                type_: "version requirement",
            })?;
        let vendor = if let Some(value) = node.get("vendor") {
            Some(
                value
                    .as_string()
                    .context(error::FieldSnafu {
                        field: "vendor",
                        type_: "string",
                    })?
                    .clone(),
            )
        } else {
            None
        };
        Ok(Self {
            addr: addr.clone(),
            kind: kind.clone(),
            name: name.clone(),
            version: version.clone(),
            vendor,
        })
    }
}
