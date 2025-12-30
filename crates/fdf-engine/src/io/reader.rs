use arrow::datatypes::Schema;
use fdf_sdk::Sample;
use std::sync::Arc;

/// Unified reader trait for different data sources
/// Returns samples one by one (generator-like API)
pub trait Reader: Iterator<Item = anyhow::Result<Sample>> {
    /// Get the schema of the data source
    fn schema(&self) -> &Arc<Schema>;
}

pub mod column_filter;
pub mod huggingface;
pub mod jsonl;
pub mod multi_file;
pub mod parquet;

pub use multi_file::MultiFileReader;
