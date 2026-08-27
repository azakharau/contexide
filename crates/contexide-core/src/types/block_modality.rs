use core::fmt;
use core::str::FromStr;
use serde::{Deserialize, Serialize};

/// Content modality of an extracted block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BlockModality {
    Text,
    Image,
    Audio,
    Video,
    Table,
    Binary,
}

/// Lowercase stable string for each variant.
impl From<BlockModality> for &'static str {
    fn from(s: BlockModality) -> Self {
        match s {
            BlockModality::Text => "text",
            BlockModality::Image => "image",
            BlockModality::Audio => "audio",
            BlockModality::Video => "video",
            BlockModality::Table => "table",
            BlockModality::Binary => "binary",
        }
    }
}

impl From<BlockModality> for String {
    fn from(s: BlockModality) -> Self {
        let lit: &'static str = s.into();
        lit.to_string()
    }
}

/// Trivial parse error (MVP). You can replace with a richer error later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockModalityParsedError;

impl fmt::Display for BlockModalityParsedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid block modality")
    }
}

impl FromStr for BlockModality {
    type Err = BlockModalityParsedError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "text" => Ok(BlockModality::Text),
            "image" => Ok(BlockModality::Image),
            "audio" => Ok(BlockModality::Audio),
            "video" => Ok(BlockModality::Video),
            "table" => Ok(BlockModality::Table),
            "binary" => Ok(BlockModality::Binary),
            _ => Err(BlockModalityParsedError),
        }
    }
}

impl core::convert::TryFrom<&str> for BlockModality {
    type Error = BlockModalityParsedError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl core::convert::TryFrom<String> for BlockModality {
    type Error = BlockModalityParsedError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.as_str().parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn block_modality_serializes() {
        let v: Value = serde_json::to_value(BlockModality::Image).unwrap();
        assert!(v.is_string());
        assert_eq!(v, json!("image"));
    }
}
