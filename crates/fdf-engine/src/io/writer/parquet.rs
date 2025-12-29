use super::Writer;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use fdf_sdk::Sample;
use parquet::arrow::ArrowWriter;
use serde_json::Value;
use std::fs::File;
use std::sync::Arc;

pub struct ParquetWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    buffer: Vec<Sample>,
    partition_size: usize,
    path: String,           // Store path for potential deletion
    samples_written: usize, // Track number of samples written
}

impl ParquetWriter {
    pub fn new(path: &str, schema: Arc<Schema>) -> anyhow::Result<Self> {
        let output_file = File::create(path)?;
        let writer = ArrowWriter::try_new(output_file, schema.clone(), None)?;
        Ok(Self {
            writer,
            schema,
            buffer: Vec::new(),
            partition_size: 10000, // Default partition size
            path: path.to_string(),
            samples_written: 0,
        })
    }

    /// Flush buffer to disk
    fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch = self.samples_to_batch(&self.buffer, &self.schema)?;
        self.samples_written += self.buffer.len();
        self.writer.write(&batch)?;
        self.buffer.clear();
        Ok(())
    }

    fn samples_to_batch(
        &self,
        samples: &[Sample],
        input_schema: &Schema,
    ) -> anyhow::Result<RecordBatch> {
        if samples.is_empty() {
            return Err(anyhow::anyhow!("Cannot create batch from empty samples"));
        }

        // Convert Sample to Value for processing
        let values: Vec<Value> = samples.iter().map(|s| s.as_value().clone()).collect();

        // Collect all field names (input + any new fields from samples)
        let mut all_field_names: Vec<String> = input_schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // Find all fields in samples
        for value in &values {
            if let Some(obj) = value.as_object() {
                for field_name in obj.keys() {
                    if !all_field_names.contains(field_name) {
                        all_field_names.push(field_name.clone());
                    }
                }
            }
        }

        // Build arrays for each field
        let mut arrays = Vec::new();
        let mut fields = Vec::new();

        for field_name in &all_field_names {
            // Determine field type from first non-null value
            let data_type = if let Some(original_field) = input_schema
                .fields()
                .iter()
                .find(|f| f.name() == field_name)
            {
                original_field.data_type().clone()
            } else {
                // Infer from first sample
                values
                    .iter()
                    .find_map(|v| v.get(field_name))
                    .map(|v| match v {
                        Value::String(_) => DataType::Utf8,
                        Value::Number(n) if n.is_i64() => DataType::Int64,
                        Value::Number(_) => DataType::Float64,
                        Value::Bool(_) => DataType::Boolean,
                        _ => DataType::Utf8,
                    })
                    .unwrap_or(DataType::Utf8)
            };

            fields.push(Field::new(field_name, data_type.clone(), true));

            // Build array
            let array: Arc<dyn arrow::array::Array> = match data_type {
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    for value in &values {
                        match value.get(field_name) {
                            Some(Value::String(s)) => builder.append_value(s),
                            Some(Value::Null) => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for value in &values {
                        match value.get(field_name) {
                            Some(Value::Number(n)) if n.is_i64() => {
                                builder.append_value(n.as_i64().unwrap())
                            }
                            Some(Value::Null) => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for value in &values {
                        match value.get(field_name) {
                            Some(Value::Number(n)) if n.is_f64() => {
                                builder.append_value(n.as_f64().unwrap())
                            }
                            Some(Value::Null) => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::new();
                    for value in &values {
                        match value.get(field_name) {
                            Some(Value::Bool(x)) => builder.append_value(*x),
                            Some(Value::Null) => builder.append_null(),
                            _ => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    return Err(anyhow::anyhow!("Unsupported data type: {:?}", data_type));
                }
            };

            arrays.push(array);
        }

        let schema = Schema::new(fields);
        Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
    }
}

impl Writer for ParquetWriter {
    fn write_sample(&mut self, sample: Sample) -> anyhow::Result<()> {
        self.buffer.push(sample);

        // Auto-flush when buffer reaches partition size
        if self.buffer.len() >= self.partition_size {
            self.flush()?;
        }

        Ok(())
    }

    fn close(mut self: Box<Self>) -> anyhow::Result<bool> {
        // Flush remaining samples
        self.flush()?;
        let has_data = self.samples_written > 0;
        self.writer.close()?;

        // If no data was written, delete the file
        if !has_data {
            let _ = std::fs::remove_file(&self.path);
        }

        Ok(has_data)
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
