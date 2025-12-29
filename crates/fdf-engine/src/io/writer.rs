use arrow::datatypes::Schema;
use fdf_sdk::Sample;
use std::sync::Arc;

/// Unified writer trait for different data sinks
/// Writer manages its own buffer and automatically flushes when buffer reaches partition size
pub trait Writer {
    /// Write a single sample
    /// Writer will automatically flush when buffer reaches partition size
    fn write_sample(&mut self, sample: Sample) -> anyhow::Result<()>;

    /// Close the writer and finalize the output
    /// This will flush any remaining samples in the buffer
    /// Returns true if any data was written, false otherwise
    fn close(self: Box<Self>) -> anyhow::Result<bool>;

    /// Get the schema
    fn schema(&self) -> &Arc<Schema>;
}

pub mod jsonl;
pub mod parquet;
pub mod sharded;
