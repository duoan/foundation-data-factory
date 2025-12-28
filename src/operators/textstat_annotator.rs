use anyhow::Result;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

use crate::operators::Operator;
use crate::operators::textstat;

pub struct TextStatAnnotator {
    column: String,
}

impl TextStatAnnotator {
    pub fn new(params: HashMap<String, serde_yaml::Value>) -> Self {
        let column = params
            .get("column")
            .and_then(|v| v.as_str())
            .unwrap_or("text")
            .to_string();

        Self { column }
    }
}

impl Operator for TextStatAnnotator {
    fn name(&self) -> &str {
        "textstat-annotator"
    }

    fn kind(&self) -> &str {
        "annotator"
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        textstat::add_textstat_annotations(batch, &self.column)
    }
}
