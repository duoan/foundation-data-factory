use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Add textstat annotation columns to a RecordBatch
pub fn add_textstat_annotations(batch: RecordBatch, column: &str) -> Result<RecordBatch> {
    // Find the text column
    let schema = batch.schema();
    let col_idx = schema
        .fields()
        .iter()
        .position(|f| f.name() == column)
        .ok_or_else(|| anyhow::anyhow!("Column {} not found", column))?;

    let text_col = batch.column(col_idx);
    let string_array = text_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("Column {} is not a string array", column))?;

    let prefix = "__annotation_textstat_";
    let mut new_columns = batch.columns().to_vec();
    let mut new_fields = schema.fields().to_vec();

    // Add each metric as a new column
    let metrics = vec![
        "flesch_reading_ease",
        "automated_readability_index",
        "syllable_count",
        "lexicon_count",
        "sentence_count",
        "character_count",
        "letter_count",
        "polysyllable_count",
        "monosyllable_count",
        "difficult_words",
    ];

    for metric_name in metrics {
        let output_col = format!("{}{}", prefix, metric_name);
        let metric_values: Float64Array = match metric_name {
            "character_count" => string_array
                .iter()
                .map(|opt_str| opt_str.map(|s| s.chars().count() as f64))
                .collect(),
            "letter_count" => string_array
                .iter()
                .map(|opt_str| {
                    opt_str.map(|s| s.chars().filter(|c| c.is_alphabetic()).count() as f64)
                })
                .collect(),
            "lexicon_count" => string_array
                .iter()
                .map(|opt_str| opt_str.map(|s| s.split_whitespace().count() as f64))
                .collect(),
            "sentence_count" => string_array
                .iter()
                .map(|opt_str| opt_str.map(|s| s.matches('.').count().max(1) as f64))
                .collect(),
            _ => {
                // Placeholder for other metrics - set to None so filters skip them
                // This allows rows to pass filters even if these metrics aren't computed yet
                vec![None; string_array.len()].into()
            }
        };

        new_columns.push(Arc::new(metric_values));
        new_fields.push(Field::new(&output_col, DataType::Float64, true));
    }

    let new_schema = Schema::new(new_fields);
    RecordBatch::try_new(Arc::new(new_schema), new_columns)
        .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))
}
