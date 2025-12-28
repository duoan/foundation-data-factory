use anyhow::Result;
use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use crate::operators::{Operator, with_column};

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
        // Get text column
        let schema = batch.schema();
        let col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name().as_str() == self.column.as_str())
            .ok_or_else(|| anyhow::anyhow!("Column {} not found", self.column))?;

        let string_array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Column {} is not a string array", self.column))?;

        let num_rows = batch.num_rows();
        let prefix = "__annotation_textstat_";

        // Convert to Vec for parallel processing
        let strings: Vec<Option<String>> = string_array
            .iter()
            .map(|opt_str| opt_str.map(|s| s.to_string()))
            .collect();

        // Calculate metrics (columnar operations)
        let character_count = Arc::new(Float64Array::from_iter(
            strings.par_iter().map(|opt_str| opt_str.as_ref().map(|s| s.chars().count() as f64)),
        ));

        let letter_count = Arc::new(Float64Array::from_iter(
            strings.par_iter().map(|opt_str| {
                opt_str
                    .as_ref()
                    .map(|s| s.chars().filter(|c| c.is_alphabetic()).count() as f64)
            }),
        ));

        let lexicon_count = Arc::new(Float64Array::from_iter(
            strings.par_iter().map(|opt_str| {
                opt_str.as_ref().map(|s| s.split_whitespace().count() as f64)
            }),
        ));

        let sentence_count = Arc::new(Float64Array::from_iter(
            strings.par_iter().map(|opt_str| {
                opt_str
                    .as_ref()
                    .map(|s| s.matches('.').count().max(1) as f64)
            }),
        ));

        // Placeholder metrics (all NULL)
        let placeholder = Arc::new(Float64Array::from(vec![None; num_rows]));

        // Add columns using with_column (like DataFrame.with_columns)
        let mut df = batch;
        df = with_column(df, &format!("{}character_count", prefix), character_count, DataType::Float64)?;
        df = with_column(df, &format!("{}letter_count", prefix), letter_count, DataType::Float64)?;
        df = with_column(df, &format!("{}lexicon_count", prefix), lexicon_count, DataType::Float64)?;
        df = with_column(df, &format!("{}sentence_count", prefix), sentence_count, DataType::Float64)?;
        df = with_column(df, &format!("{}flesch_reading_ease", prefix), placeholder.clone(), DataType::Float64)?;
        df = with_column(df, &format!("{}automated_readability_index", prefix), placeholder.clone(), DataType::Float64)?;
        df = with_column(df, &format!("{}syllable_count", prefix), placeholder.clone(), DataType::Float64)?;
        df = with_column(df, &format!("{}polysyllable_count", prefix), placeholder.clone(), DataType::Float64)?;
        df = with_column(df, &format!("{}monosyllable_count", prefix), placeholder.clone(), DataType::Float64)?;
        df = with_column(df, &format!("{}difficult_words", prefix), placeholder, DataType::Float64)?;

        Ok(df)
    }
}
