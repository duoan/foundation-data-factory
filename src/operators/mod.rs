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

    /// Default implementation: apply annotations to a batch
    /// This converts batch to rows, annotates each row, and converts back
    fn apply_annotations(&self, batch: RecordBatch) -> Result<RecordBatch> {
        use crate::operators::row::{batch_to_rows, rows_to_batch};
        use rayon::prelude::*;

        // Convert batch to rows
        let rows = batch_to_rows(&batch)?;

        // Annotate rows in parallel using Rayon
        let annotated_rows: Result<Vec<_>> = rows
            .into_par_iter()
            .map(|row| self.annotate(&row))
            .collect();

        // Convert back to batch (schema will be updated with new annotation columns)
        rows_to_batch(&annotated_rows?, &batch.schema())
    }
}

/// Base trait for filters - operators that filter rows based on conditions
/// Filters work directly on Arrow arrays (columnar), not row-by-row
pub trait FilterBase: Send + Sync {
    /// Build a filter mask for the entire batch
    /// This works directly on Arrow arrays (columnar) for efficiency
    /// Returns a BooleanArray where true means keep the row, false means filter out
    fn build_filter_mask(&self, batch: &RecordBatch) -> Result<arrow::array::BooleanArray>;
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
