use core::fmt;
use core::str::FromStr;
use serde::{Deserialize, Serialize};

/// Where the asset came from (ingress).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AssetSource {
    Upload,
    Url,
    S3,
}

impl From<AssetSource> for &'static str {
    fn from(s: AssetSource) -> Self {
        match s {
            AssetSource::Upload => "upload",
            AssetSource::Url => "url",
            AssetSource::S3 => "s3",
        }
    }
}

impl From<AssetSource> for String {
    fn from(s: AssetSource) -> Self {
        let lit: &'static str = s.into();
        lit.to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssetSourceError;

impl fmt::Display for AssetSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid asset source")
    }
}

impl FromStr for AssetSource {
    type Err = AssetSourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "upload" => Ok(AssetSource::Upload),
            "url" => Ok(AssetSource::Url),
            "s3" => Ok(AssetSource::S3),
            _ => Err(AssetSourceError),
        }
    }
}
