use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub pipeline_name: String,
    pub stages: Vec<StageManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageManifest {
    pub name: String,
    pub input_path: Option<String>,
    pub output_path: String,
    pub partition_files: Vec<String>,
    pub operators: Vec<OperatorManifest>,
    pub total_input_rows: usize,
    pub total_output_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorManifest {
    pub name: String,
    pub kind: String,
    pub input_rows: usize,
    pub output_rows: usize,
    pub filtered_rows: Option<usize>, // For filter operators
}

impl Manifest {
    pub fn new(pipeline_name: String) -> Self {
        Self {
            pipeline_name,
            stages: Vec::new(),
        }
    }

    pub fn add_stage(&mut self, stage: StageManifest) {
        self.stages.push(stage);
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path.as_ref(), json)?;
        Ok(())
    }
}
