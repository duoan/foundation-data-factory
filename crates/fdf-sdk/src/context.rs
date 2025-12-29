use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context {
    pub job_id: String,
    pub shard_id: Option<String>,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            job_id: "default".to_string(),
            shard_id: None,
        }
    }
}
