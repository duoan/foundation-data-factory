use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub source: SourceSpec,
    pub pipeline: Vec<OpNode>,
    pub sink: SinkSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSpec {
    pub kind: String,
    pub uris: Vec<String>,
    #[serde(default)]
    pub columns: ColumnMapping,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ColumnMapping {
    #[serde(flatten)]
    pub mapping: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpNode {
    pub op: String,
    #[serde(default)]
    pub config: serde_yaml::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkSpec {
    pub kind: String,
    pub uri: String,
    #[serde(default = "default_mode")]
    pub mode: String,
}

fn default_mode() -> String {
    "overwrite".to_string()
}
