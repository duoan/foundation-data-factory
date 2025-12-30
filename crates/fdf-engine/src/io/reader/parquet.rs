use super::Reader;
use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fdf_sdk::Sample;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

pub struct ParquetReader {
    reader: ::parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    schema: Arc<Schema>,
    current_batch: Option<RecordBatch>,
    current_row: usize,
    column_rename: Option<HashMap<usize, String>>, // column_index -> new_name
}

impl ParquetReader {
    /// Create a new ParquetReader from a file path
    ///
    /// # Arguments
    /// * `path` - Path to the parquet file
    /// * `batch_size` - Optional batch size for reading. If None, uses default batch size.
    pub fn new(path: &str) -> anyhow::Result<Self> {
        Self::with_batch_size(path, None)
    }

    /// Create a new ParquetReader from a file path with a specific batch size
    ///
    /// # Arguments
    /// * `path` - Path to the parquet file
    /// * `batch_size` - Optional batch size for reading. If None, uses default batch size.
    pub fn with_batch_size(path: &str, batch_size: Option<usize>) -> anyhow::Result<Self> {
        Self::with_options(path, batch_size, None)
    }

    /// Create a new ParquetReader with column projection
    ///
    /// # Arguments
    /// * `path` - Path to the parquet file
    /// * `batch_size` - Optional batch size for reading
    /// * `column_mapping` - Optional column mapping (new_name -> original_name). If provided, only reads specified columns.
    pub fn with_options(
        path: &str,
        batch_size: Option<usize>,
        column_mapping: Option<std::collections::HashMap<String, String>>,
    ) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Set batch size if provided
        if let Some(size) = batch_size {
            builder = builder.with_batch_size(size);
        }

        let original_schema = builder.schema().clone();

        // Apply column projection if column mapping is provided
        let (schema, column_rename) = if let Some(mapping) = column_mapping {
            if mapping.is_empty() {
                (original_schema.clone(), None)
            } else {
                // Build projection: get indices of columns to read
                let mut projection_indices = Vec::new();
                let mut column_rename: HashMap<usize, String> = HashMap::new();
                let mut new_fields = Vec::new();

                for (new_name, original_name) in &mapping {
                    if let Some((idx, field)) = original_schema
                        .fields()
                        .iter()
                        .enumerate()
                        .find(|(_, f)| f.name() == original_name)
                    {
                        projection_indices.push(idx);
                        column_rename.insert(projection_indices.len() - 1, new_name.clone());
                        new_fields.push(Field::new(
                            new_name.clone(),
                            field.data_type().clone(),
                            field.is_nullable(),
                        ));
                    } else {
                        return Err(anyhow::anyhow!(
                            "Column '{}' not found in parquet file. Available columns: {:?}",
                            original_name,
                            original_schema
                                .fields()
                                .iter()
                                .map(|f| f.name())
                                .collect::<Vec<_>>()
                        ));
                    }
                }

                // Apply projection using ProjectionMask
                // Get the parquet file's schema descriptor from the builder
                let parquet_metadata = builder.metadata().clone();
                let schema_desc = parquet_metadata.file_metadata().schema_descr();
                let projection_mask = ::parquet::arrow::ProjectionMask::leaves(
                    schema_desc,
                    projection_indices.clone(),
                );
                builder = builder.with_projection(projection_mask);

                (Arc::new(Schema::new(new_fields)), Some(column_rename))
            }
        } else {
            (original_schema.clone(), None)
        };

        let reader = builder.build()?;

        Ok(Self {
            reader,
            schema,
            current_batch: None,
            current_row: 0,
            column_rename,
        })
    }

    /// Convert a row from RecordBatch to Sample
    /// Optimized for performance: pre-allocates HashMap and reduces string allocations
    fn row_to_sample(&self, batch: &RecordBatch, row_idx: usize) -> Sample {
        use serde_json::Map;

        // Pre-allocate HashMap with known capacity to reduce reallocations
        let field_count = self.schema.fields().len();
        let mut map = Map::with_capacity(field_count);

        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            let array = batch.column(col_idx);

            // Determine column name (use rename if available, otherwise use field name)
            // Cache field names to avoid repeated lookups
            let col_name: String = if let Some(ref rename_map) = self.column_rename {
                rename_map
                    .get(&col_idx)
                    .cloned()
                    .unwrap_or_else(|| field.name().clone())
            } else {
                field.name().clone()
            };

            let value = match field.data_type() {
                DataType::Utf8 | DataType::LargeUtf8 => {
                    if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                        if arr.is_null(row_idx) {
                            Value::Null
                        } else {
                            // Direct string slice to avoid unnecessary allocation
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
                                    .unwrap_or_else(|| serde_json::Number::from(0)),
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
            map.insert(col_name, value);
        }

        Sample(Value::Object(map))
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
