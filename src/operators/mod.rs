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
/// Filters work on individual rows, not Arrow RecordBatches
pub trait FilterBase: Send + Sync {
    /// Determine if a row should be kept
    /// Returns true if the row should be kept, false if it should be filtered out
    fn should_keep(&self, row: &Row) -> Result<bool>;

    /// Default implementation: apply filter to a batch
    /// This converts batch to rows, checks each row, and applies the filter
    fn apply_filter(&self, batch: RecordBatch) -> Result<RecordBatch> {
        use crate::operators::row::batch_to_rows;
        use arrow::array::BooleanArray;
        use arrow::compute::filter_record_batch;
        use rayon::prelude::*;

        // Convert batch to rows
        let rows = batch_to_rows(&batch)?;

        // Build filter mask by checking each row in parallel
        let keep_mask: Result<Vec<bool>> = rows
            .par_iter()
            .map(|row| self.should_keep(row))
            .collect();

        // Convert to BooleanArray and apply filter
        let boolean_array = BooleanArray::from(keep_mask?);
        Ok(filter_record_batch(&batch, &boolean_array)?)
    }
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
