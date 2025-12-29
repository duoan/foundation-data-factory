use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub source: SourceSpec,
    pub pipeline: Vec<OperatorNode>,
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

#[derive(Debug, Clone)]
pub struct OperatorNode {
    pub name: String,
    pub config: serde_yaml::Value,
}

impl Serialize for OperatorNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.name, &self.config)?;
        map.end()
    }
}

impl<'de> Deserialize<'de> for OperatorNode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Support both formats:
        // 1. Simplified: { "text.normalize": { "text_col": "text", ... } }
        // 2. Legacy: { "name": "text.normalize", "config": { ... } }

        let value: serde_yaml::Value = Deserialize::deserialize(deserializer)?;

        match value {
            serde_yaml::Value::Mapping(map) => {
                // Check if it's the legacy format with "name" and "config" keys
                if let (Some(serde_yaml::Value::String(name)), Some(config)) = (
                    map.get(serde_yaml::Value::String("name".to_string())),
                    map.get(serde_yaml::Value::String("config".to_string())),
                ) {
                    return Ok(OperatorNode {
                        name: name.clone(),
                        config: config.clone(),
                    });
                }

                // Otherwise, treat it as simplified format: single key-value pair
                if map.len() == 1 {
                    let (name_val, config_val) = map
                        .iter()
                        .next()
                        .ok_or_else(|| serde::de::Error::custom("Empty operator node"))?;

                    let name = name_val
                        .as_str()
                        .ok_or_else(|| serde::de::Error::custom("Operator name must be a string"))?
                        .to_string();

                    return Ok(OperatorNode {
                        name,
                        config: config_val.clone(),
                    });
                }

                Err(serde::de::Error::custom(
                    "Operator node must have exactly one key (operator name) or use legacy format with 'name' and 'config'"
                ))
            }
            _ => Err(serde::de::Error::custom("Operator node must be a mapping")),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkSpec {
    pub kind: String,
    pub uri: String, // Base output URI
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default)]
    pub shard_key: Option<String>, // Field name to use for sharding
    #[serde(default = "default_samples_per_shard")]
    pub samples_per_shard: usize, // Number of samples per shard
    #[serde(default)]
    pub shard_name_pattern: Option<String>, // Pattern for shard file names, e.g., "{base}.part-{shard_id:08}.{ext}" or "{base}-{shard_id:04d}.{ext}"
                                            // Trace and error outputs are always enabled by default
                                            // Trace: automatically creates {uri}/trace/step_xx/ and {uri}/final/
                                            // Error: automatically creates {uri}/error/
}

fn default_mode() -> String {
    "overwrite".to_string()
}

fn default_samples_per_shard() -> usize {
    10000
}
