use serde::{Deserialize, Serialize};

/// Pipeline stages (used in logs/metrics/events).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Stage {
    Fetch,
    Extract,
    Clean,
    Chunk,
    Embed,
    Persist,
}
