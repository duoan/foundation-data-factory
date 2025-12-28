use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

use crate::operators::{AnnotatorBase, Row, Value};

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

impl AnnotatorBase for TextStatAnnotator {
    fn annotate(&self, row: &Row) -> Result<Row> {
        // Get the text value
        let text = row.get_string(&self.column).ok_or_else(|| {
            anyhow::anyhow!("Column {} not found or is not a string", self.column)
        })?;

        // Create a new row with all original values plus annotations
        let mut new_values = row.values.clone();

        // Calculate metrics
        let prefix = "__annotation_textstat_";

        // Character count
        new_values.insert(
            format!("{}character_count", prefix),
            Value::Float64(text.chars().count() as f64),
        );

        // Letter count
        new_values.insert(
            format!("{}letter_count", prefix),
            Value::Float64(text.chars().filter(|c| c.is_alphabetic()).count() as f64),
        );

        // Lexicon count (word count)
        new_values.insert(
            format!("{}lexicon_count", prefix),
            Value::Float64(text.split_whitespace().count() as f64),
        );

        // Sentence count
        new_values.insert(
            format!("{}sentence_count", prefix),
            Value::Float64(text.matches('.').count().max(1) as f64),
        );

        // Placeholder for other metrics (set to Null so filters skip them)
        let placeholder_metrics = vec![
            "flesch_reading_ease",
            "automated_readability_index",
            "syllable_count",
            "polysyllable_count",
            "monosyllable_count",
            "difficult_words",
        ];

        for metric_name in placeholder_metrics {
            new_values.insert(format!("{}{}", prefix, metric_name), Value::Null);
        }

        Ok(Row { values: new_values })
    }
}

impl_operator! {
    TextStatAnnotator,
    name: "textstat-annotator",
    kind: "annotator",
    apply: |self, batch| {
        // Optimized: directly work on Arrow arrays to avoid memory copies
        use rayon::prelude::*;

        // Find the text column
        let schema = batch.schema();
        let col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name().as_str() == self.column.as_str())
            .ok_or_else(|| anyhow::anyhow!("Column {} not found", self.column))?;

        let text_col = batch.column(col_idx);
        let string_array = text_col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("Column {} is not a string array", self.column))?;

        let num_rows = batch.num_rows();
        let prefix = "__annotation_textstat_";

        // Calculate metrics in parallel directly on Arrow arrays
        // Convert to Vec first for parallel processing
        let strings: Vec<Option<String>> = string_array
            .iter()
            .map(|opt_str| opt_str.map(|s| s.to_string()))
            .collect();

        let character_count: Vec<Option<f64>> = strings
            .par_iter()
            .map(|opt_str| opt_str.as_ref().map(|s| s.chars().count() as f64))
            .collect();
        let character_count = Float64Array::from_iter(character_count);

        let letter_count: Vec<Option<f64>> = strings
            .par_iter()
            .map(|opt_str| {
                opt_str.as_ref().map(|s| s.chars().filter(|c| c.is_alphabetic()).count() as f64)
            })
            .collect();
        let letter_count = Float64Array::from_iter(letter_count);

        let lexicon_count: Vec<Option<f64>> = strings
            .par_iter()
            .map(|opt_str| opt_str.as_ref().map(|s| s.split_whitespace().count() as f64))
            .collect();
        let lexicon_count = Float64Array::from_iter(lexicon_count);

        let sentence_count: Vec<Option<f64>> = strings
            .par_iter()
            .map(|opt_str| opt_str.as_ref().map(|s| s.matches('.').count().max(1) as f64))
            .collect();
        let sentence_count = Float64Array::from_iter(sentence_count);

        // Placeholder metrics (all None) - create once and reuse
        let placeholder_vec: Vec<Option<f64>> = vec![None; num_rows];

        // Build new columns and schema
        let mut new_columns = batch.columns().to_vec();
        let mut new_fields = schema.fields().to_vec();

        // Add annotation columns
        let placeholder = Float64Array::from_iter(placeholder_vec.clone());
        let metrics = vec![
            ("character_count", character_count),
            ("letter_count", letter_count),
            ("lexicon_count", lexicon_count),
            ("sentence_count", sentence_count),
            ("flesch_reading_ease", Float64Array::from_iter(placeholder_vec.clone())),
            ("automated_readability_index", Float64Array::from_iter(placeholder_vec.clone())),
            ("syllable_count", Float64Array::from_iter(placeholder_vec.clone())),
            ("polysyllable_count", Float64Array::from_iter(placeholder_vec.clone())),
            ("monosyllable_count", Float64Array::from_iter(placeholder_vec.clone())),
            ("difficult_words", placeholder),
        ];

        for (metric_name, metric_array) in metrics {
            let output_col = format!("{}{}", prefix, metric_name);
            new_columns.push(Arc::new(metric_array));
            new_fields.push(Field::new(&output_col, DataType::Float64, true));
        }

        let new_schema = Schema::new(new_fields);
        RecordBatch::try_new(Arc::new(new_schema), new_columns)
            .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))
    }
}
