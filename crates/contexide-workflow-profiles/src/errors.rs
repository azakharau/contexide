use contexide_core::errors::Error;
use thiserror::Error;

/// Errors specific to workflow profiles.
#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("missing document_id in input")]
    MissingDocumentId,
    #[error("no assets provided")]
    NoAssets,
}

impl From<ProfileError> for Error {
    fn from(err: ProfileError) -> Self {
        Error::Other(err.into())
    }
}
