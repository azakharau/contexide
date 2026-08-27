pub(crate) mod file;
pub(crate) mod http;
pub(crate) mod pdf;
pub mod router;
// Contracts live in `contexide-core::extractor`.

pub use contexide_core::extractor::{AssetInput, ExtractContext, ExtractedBlock, Extractor};
