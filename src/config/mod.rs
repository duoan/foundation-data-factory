use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub name: String,
    pub stages: Vec<StageConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageConfig {
    pub name: String,
    pub input: Option<InputConfig>,
    pub operators: Vec<OperatorConfig>,
    pub output: OutputConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    pub source: DataSourceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    pub source: DataSourceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceConfig {
    #[serde(rename = "type")]
    pub source_type: String,
    pub path: Option<String>,
    pub streaming: Option<bool>,
    pub token: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OperatorConfig {
    Simple(String),
    WithParams {
        #[serde(flatten)]
        params: HashMap<String, serde_yaml::Value>,
    },
}

impl PipelineConfig {
    pub fn from_yaml_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;
        Self::from_yaml_str(&content)
    }

    pub fn from_yaml_str(content: &str) -> Result<Self> {
        let config: PipelineConfig =
            serde_yaml::from_str(content).context("Failed to parse YAML configuration")?;

        // Validate
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        // Check stage names are unique
        let mut stage_names = std::collections::HashSet::new();
        for stage in &self.stages {
            if !stage_names.insert(&stage.name) {
                anyhow::bail!("Duplicate stage name: {}", stage.name);
            }
        }

        // Check first stage has input
        if let Some(first_stage) = self.stages.first() {
            if first_stage.input.is_none() {
                anyhow::bail!(
                    "First stage '{}' must have input configuration",
                    first_stage.name
                );
            }
        }

        Ok(())
    }
}

impl OperatorConfig {
    pub fn get_operator_name(&self) -> String {
        match self {
            OperatorConfig::Simple(name) => name.clone(),
            OperatorConfig::WithParams { params } => {
                // Find the operator name (the key that's not a standard field)
                for (key, _) in params {
                    if !matches!(key.as_str(), "id" | "kind" | "op" | "params") {
                        return key.clone();
                    }
                }
                // Fallback: try to get from "op" field if present
                params
                    .get("op")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            }
        }
    }

    pub fn get_params(&self) -> HashMap<String, serde_yaml::Value> {
        match self {
            OperatorConfig::Simple(_) => HashMap::new(),
            OperatorConfig::WithParams { params } => {
                // Extract nested params if operator name is the key
                let mut result = HashMap::new();
                for (key, value) in params {
                    if !matches!(key.as_str(), "id" | "kind" | "op" | "params") {
                        // This is the operator name, extract its params
                        if let Some(nested) = value.as_mapping() {
                            for (k, v) in nested {
                                if let (Some(k_str), _) = (k.as_str(), v) {
                                    result.insert(k_str.to_string(), v.clone());
                                }
                            }
                        }
                    } else if key == "params" {
                        // Direct params field
                        if let Some(nested) = value.as_mapping() {
                            for (k, v) in nested {
                                if let (Some(k_str), _) = (k.as_str(), v) {
                                    result.insert(k_str.to_string(), v.clone());
                                }
                            }
                        }
                    }
                }
                result
            }
        }
    }
}
