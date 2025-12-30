use super::Reader;
use arrow::datatypes::Schema;
use fdf_sdk::Sample;
use std::sync::Arc;

/// A reader that wraps multiple readers and reads from them sequentially
pub struct MultiFileReader {
    readers: Vec<Box<dyn Reader>>,
    current_reader_index: usize,
    schema: Arc<Schema>, // Schema from the first reader (all readers should have the same schema)
}

impl MultiFileReader {
    /// Create a new MultiFileReader from a list of readers
    pub fn new(readers: Vec<Box<dyn Reader>>) -> anyhow::Result<Self> {
        if readers.is_empty() {
            return Err(anyhow::anyhow!(
                "MultiFileReader requires at least one reader"
            ));
        }

        // Use the schema from the first reader
        let schema = readers[0].schema().clone();

        // Validate that all readers have compatible schemas
        for (idx, reader) in readers.iter().enumerate().skip(1) {
            let other_schema = reader.schema();
            if !schemas_compatible(&schema, other_schema) {
                return Err(anyhow::anyhow!(
                    "Reader {} has incompatible schema with the first reader",
                    idx
                ));
            }
        }

        Ok(Self {
            readers,
            current_reader_index: 0,
            schema,
        })
    }
}

/// Check if two schemas are compatible (same field names and types)
fn schemas_compatible(schema1: &Schema, schema2: &Schema) -> bool {
    if schema1.fields().len() != schema2.fields().len() {
        return false;
    }

    for (field1, field2) in schema1.fields().iter().zip(schema2.fields().iter()) {
        if field1.name() != field2.name() || field1.data_type() != field2.data_type() {
            return false;
        }
    }

    true
}

impl Iterator for MultiFileReader {
    type Item = anyhow::Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        // Try to get a sample from the current reader
        while self.current_reader_index < self.readers.len() {
            if let Some(result) = self.readers[self.current_reader_index].next() {
                return Some(result);
            }

            // Current reader is exhausted, move to next
            self.current_reader_index += 1;
        }

        // All readers are exhausted
        None
    }
}

impl Reader for MultiFileReader {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
