use super::Reader;
use arrow::datatypes::{DataType, Field, Schema};
use fdf_sdk::Sample;
use serde_json::Value;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;

pub struct JsonlReader {
    reader: BufReader<File>,
    schema: Arc<Schema>,
    current_line: Option<String>,
}

impl JsonlReader {
    /// Create a new JsonlReader from a file path
    /// The schema is inferred from the first line
    pub fn new(path: &str) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut first_line = String::new();

        // Read first line to infer schema
        reader.read_line(&mut first_line)?;

        let schema = if first_line.trim().is_empty() {
            // Empty file, create empty schema
            Arc::new(Schema::empty())
        } else {
            // Parse first JSON object to infer schema
            let first_json: Value = serde_json::from_str(&first_line)?;
            Self::infer_schema(&first_json)
        };

        Ok(Self {
            reader,
            schema,
            current_line: Some(first_line),
        })
    }

    /// Infer schema from a JSON value
    fn infer_schema(value: &Value) -> Arc<Schema> {
        if let Value::Object(map) = value {
            let fields: Vec<Field> = map
                .iter()
                .map(|(name, val)| {
                    let data_type = match val {
                        Value::String(_) => DataType::Utf8,
                        Value::Number(n) if n.is_i64() => DataType::Int64,
                        Value::Number(_) => DataType::Float64,
                        Value::Bool(_) => DataType::Boolean,
                        _ => DataType::Utf8, // Default to string for arrays/objects/null
                    };
                    Field::new(name, data_type, true)
                })
                .collect();
            Arc::new(Schema::new(fields))
        } else {
            Arc::new(Schema::empty())
        }
    }

    /// Convert JSON value to Sample
    fn json_to_sample(&self, value: Value) -> Sample {
        Sample::from_value(value).unwrap_or_default()
    }
}

impl Iterator for JsonlReader {
    type Item = anyhow::Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        // Return current line if available
        if let Some(line) = self.current_line.take() {
            if line.trim().is_empty() {
                return None;
            }
            match serde_json::from_str::<Value>(&line) {
                Ok(value) => {
                    let sample = self.json_to_sample(value);
                    return Some(Ok(sample));
                }
                Err(e) => return Some(Err(anyhow::anyhow!("Failed to parse JSON: {}", e))),
            }
        }

        // Read next line
        let mut line = String::new();
        match self.reader.read_line(&mut line) {
            Ok(0) => None, // EOF
            Ok(_) => {
                if line.trim().is_empty() {
                    None
                } else {
                    match serde_json::from_str::<Value>(&line) {
                        Ok(value) => {
                            let sample = self.json_to_sample(value);
                            Some(Ok(sample))
                        }
                        Err(e) => Some(Err(anyhow::anyhow!("Failed to parse JSON: {}", e))),
                    }
                }
            }
            Err(e) => Some(Err(anyhow::anyhow!("Failed to read line: {}", e))),
        }
    }
}

impl Reader for JsonlReader {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
