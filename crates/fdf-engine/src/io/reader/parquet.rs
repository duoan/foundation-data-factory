use super::Reader;
use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::array::*;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use fdf_sdk::Sample;
use serde_json::Value;
use std::fs::File;
use std::sync::Arc;

pub struct ParquetReader {
    reader: ::parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    schema: Arc<Schema>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
}

impl ParquetReader {
    /// Create a new ParquetReader from a file path
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();
        let reader = builder.build()?;

        Ok(Self {
            reader,
            schema,
            current_batch: None,
            current_row: 0,
        })
    }

    /// Convert a row from RecordBatch to Sample
    fn row_to_sample(&self, batch: &RecordBatch, row_idx: usize) -> Sample {
        let mut sample = Sample::new();
        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            let array = batch.column(col_idx);
            let value = match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                        if arr.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::String(arr.value(row_idx).to_string())
                        }
                    } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                        if arr.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::String(arr.value(row_idx).to_string())
                        }
                    } else {
                        Value::Null
                    }
                }
                DataType::Int64 => {
                    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                        if arr.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::Number(arr.value(row_idx).into())
                        }
                    } else {
                        Value::Null
                    }
                }
                DataType::Float64 => {
                    if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                        if arr.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::Number(
                                serde_json::Number::from_f64(arr.value(row_idx))
                                    .unwrap_or(serde_json::Number::from(0)),
                            )
                        }
                    } else {
                        Value::Null
                    }
                }
                DataType::Boolean => {
                    if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                        if arr.is_null(row_idx) {
                            Value::Null
                        } else {
                            Value::Bool(arr.value(row_idx))
                        }
                    } else {
                        Value::Null
                    }
                }
                _ => Value::Null,
            };
            sample.set_value(field.name().clone(), value);
        }
        sample
    }

    /// Load the next batch if needed
    fn ensure_batch(&mut self) -> anyhow::Result<bool> {
        // If we have a batch and haven't exhausted it, return true
        if let Some(ref batch) = self.current_batch {
            if self.current_row < batch.num_rows() {
                return Ok(true);
            }
        }

        // Try to load next batch
        match self.reader.next() {
            Some(Ok(batch)) => {
                self.current_batch = Some(batch);
                self.current_row = 0;
                Ok(true)
            }
            Some(Err(e)) => Err(anyhow::anyhow!("Error reading batch: {}", e)),
            None => Ok(false), // No more batches
        }
    }
}

impl Iterator for ParquetReader {
    type Item = anyhow::Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        // Ensure we have a batch to read from
        match self.ensure_batch() {
            Ok(true) => {
                // We have a batch, get the current row
                if let Some(ref batch) = self.current_batch {
                    let sample = self.row_to_sample(batch, self.current_row);
                    self.current_row += 1;
                    Some(Ok(sample))
                } else {
                    None
                }
            }
            Ok(false) => None, // No more batches
            Err(e) => Some(Err(e)),
        }
    }
}

impl Reader for ParquetReader {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
