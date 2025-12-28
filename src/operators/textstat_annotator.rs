use anyhow::Result;
use std::collections::HashMap;

use crate::operators::{AnnotatorBase, Operator, Row, Value};

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
        use crate::operators::row::{batch_to_rows, rows_to_batch};

        // Convert batch to rows
        let mut rows = batch_to_rows(&batch)?;

        // Annotate each row
        for row in &mut rows {
            *row = <Self as AnnotatorBase>::annotate(self, row)?;
        }

        // Convert back to batch (need to update schema with new annotation columns)
        // For now, we'll use a simplified approach - in production, you'd want to
        // properly update the schema
        rows_to_batch(&rows, &batch.schema())
    }
}
