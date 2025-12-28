use anyhow::Result;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

pub mod textstat_annotator;
pub mod textstat_filter;

/// Core operator trait
pub trait Operator: Send + Sync {
    fn name(&self) -> &str;
    fn kind(&self) -> &str;
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch>;
}

/// Helper: Add a column to a RecordBatch
pub fn with_column(
    batch: RecordBatch,
    name: &str,
    array: arrow::array::ArrayRef,
    data_type: arrow::datatypes::DataType,
) -> Result<RecordBatch> {
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    let schema = batch.schema();
    let mut new_columns = batch.columns().to_vec();
    let mut new_fields = schema.fields().to_vec();

    new_columns.push(array);
    new_fields.push(Field::new(name, data_type, true));

    let new_schema = Schema::new(new_fields);
    RecordBatch::try_new(Arc::new(new_schema), new_columns)
        .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))
}

/// Helper: Filter rows using a boolean mask
pub fn where_filter(batch: RecordBatch, mask: &arrow::array::BooleanArray) -> Result<RecordBatch> {
    Ok(arrow::compute::filter_record_batch(&batch, mask)?)
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
