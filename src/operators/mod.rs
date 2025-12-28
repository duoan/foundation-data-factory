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
