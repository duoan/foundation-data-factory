use anyhow::Result;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

#[macro_use]
mod macros;
mod row;

pub mod textstat_annotator;
pub mod textstat_filter;

pub use row::{Row, Value};

pub trait Operator: Send + Sync {
    #[allow(dead_code)] // May be used for logging/debugging in the future
    fn name(&self) -> &str;
    fn kind(&self) -> &str;
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch>;
}

/// Base trait for annotators - operators that add annotation fields without dropping rows
/// Annotators work on individual rows, not Arrow RecordBatches
pub trait AnnotatorBase: Send + Sync {
    /// Add annotation fields to a single row
    /// Returns a new row with additional annotation fields
    fn annotate(&self, row: &Row) -> Result<Row>;
}

/// Base trait for filters - operators that filter rows based on conditions
/// Filters work on individual rows, not Arrow RecordBatches
pub trait FilterBase: Send + Sync {
    /// Determine if a row should be kept
    /// Returns true if the row should be kept, false if it should be filtered out
    fn should_keep(&self, row: &Row) -> Result<bool>;
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
