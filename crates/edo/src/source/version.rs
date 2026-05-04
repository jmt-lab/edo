use std::fmt;

use resolvo::utils::VersionSet;
use semver::{Version, VersionReq};

/// A versioned package record associated with a specific vendor.
///
/// Used internally by the resolver to track which vendor provides each
/// candidate version.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct EdoVersion {
    vendor: String,
    version: Version,
}

impl EdoVersion {
    /// Create a new version record for the given vendor and semver version.
    pub fn new(vendor: &str, version: &Version) -> Self {
        Self {
            vendor: vendor.to_string(),
            version: version.clone(),
        }
    }

    /// Return the vendor name that provides this version.
    pub fn vendor(&self) -> String {
        self.vendor.clone()
    }

    /// Return the semver version.
    pub fn version(&self) -> Version {
        self.version.clone()
    }

    /// Check whether this version satisfies the given semver requirement.
    pub fn matches(&self, require: &VersionReq) -> bool {
        require.matches(&self.version)
    }
}

impl fmt::Display for EdoVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("{}-{}", self.vendor, self.version))
    }
}

/// A set of [`EdoVersion`] candidates that the resolver treats as a single version set.
///
/// Implements [`resolvo::utils::VersionSet`] so it can be interned directly
/// into the resolver pool.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct EdoVersionSet(Vec<EdoVersion>);

unsafe impl Send for EdoVersionSet {}
unsafe impl Sync for EdoVersionSet {}

impl EdoVersionSet {
    /// Create a version set from the given slice of versions.
    pub fn new(input: &[EdoVersion]) -> Self {
        Self(input.to_vec())
    }

    /// Return the contained versions as a slice.
    pub fn get(&self) -> &[EdoVersion] {
        self.0.as_slice()
    }
}

impl fmt::Display for EdoVersionSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "{}",
            self.0
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ))
    }
}

impl VersionSet for EdoVersionSet {
    type V = EdoVersion;
}
