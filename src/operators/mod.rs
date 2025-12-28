use anyhow::Result;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

#[macro_use]
mod macros;

pub mod textstat_annotator;
pub mod textstat_filter;

pub trait Operator: Send + Sync {
    #[allow(dead_code)] // May be used for logging/debugging in the future
    fn name(&self) -> &str;
    fn kind(&self) -> &str;
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch>;
}

/// Base trait for annotators - operators that add annotation fields without dropping rows
pub trait AnnotatorBase: Send + Sync {
    /// Add annotation columns to a batch
    /// This should not drop any rows, only add new columns
    fn add_annotations(&self, batch: RecordBatch) -> Result<RecordBatch>;
}

/// Base trait for filters - operators that filter rows based on conditions
pub trait FilterBase: Send + Sync {
    /// Determine if a row should be kept based on the given row index and batch
    /// Returns true if the row should be kept, false if it should be filtered out
    fn should_keep(&self, batch: &RecordBatch, row_idx: usize) -> Result<bool>;
}

pub fn create_operator(
    name: &str,
    params: HashMap<String, serde_yaml::Value>,
) -> Result<Box<dyn Operator>> {
    match name {
        "textstat-annotator" => Ok(Box::new(textstat_annotator::TextStatAnnotator::new(params))),
        "textstat-filter" => Ok(Box::new(textstat_filter::TextStatFilter::new(params)?)),
        _ => anyhow::bail!("Unknown operator: {}", name),
    }
}
