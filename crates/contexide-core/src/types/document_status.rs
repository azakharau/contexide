use core::fmt;
use core::str::FromStr;
use serde::{Deserialize, Serialize};

/// Lifecycle state of a document in the pipeline.
///
/// JSON uses lowercase variants for readability/stability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DocumentStatus {
    Draft,
    Processing,
    Ready,
    Failed,
    Archived,
}

/// Lowercase stable string for each variant.
impl From<DocumentStatus> for &'static str {
    fn from(s: DocumentStatus) -> Self {
        match s {
            DocumentStatus::Draft => "draft",
            DocumentStatus::Processing => "processing",
            DocumentStatus::Ready => "ready",
            DocumentStatus::Failed => "failed",
            DocumentStatus::Archived => "archived",
        }
    }
}

impl From<DocumentStatus> for String {
    fn from(s: DocumentStatus) -> Self {
        let lit: &'static str = s.into();
        lit.to_string()
    }
}

/// Trivial parse error (MVP). You can replace with a richer error later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseDocumentStatusError;

impl fmt::Display for ParseDocumentStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid document status")
    }
}

impl FromStr for DocumentStatus {
    type Err = ParseDocumentStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "draft" => Ok(DocumentStatus::Draft),
            "processing" => Ok(DocumentStatus::Processing),
            "ready" => Ok(DocumentStatus::Ready),
            "failed" => Ok(DocumentStatus::Failed),
            "archived" => Ok(DocumentStatus::Archived),
            _ => Err(ParseDocumentStatusError),
        }
    }
}

impl core::convert::TryFrom<&str> for DocumentStatus {
    type Error = ParseDocumentStatusError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl core::convert::TryFrom<String> for DocumentStatus {
    type Error = ParseDocumentStatusError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.as_str().parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_status_json_is_lowercase() {
        let s = serde_json::to_string(&DocumentStatus::Processing).unwrap();
        assert_eq!(s, "\"processing\"");
        let v: DocumentStatus = serde_json::from_str(&s).unwrap();
        assert_eq!(v, DocumentStatus::Processing);
    }
}
