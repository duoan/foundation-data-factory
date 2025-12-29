use arrow::datatypes::Schema;
use fdf_sdk::Sample;
use std::sync::Arc;

/// Unified reader trait for different data sources
/// Returns samples one by one (generator-like API)
pub trait Reader: Iterator<Item = anyhow::Result<Sample>> {
    /// Get the schema of the data source
    fn schema(&self) -> &Arc<Schema>;
}

pub mod jsonl;
pub mod parquet;
