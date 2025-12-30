use super::Reader;
use arrow::datatypes::{Field, Schema};
use fdf_sdk::Sample;
use std::collections::HashMap;
use std::sync::Arc;

/// A reader that filters and renames columns based on ColumnMapping
pub struct ColumnFilterReader {
    inner: Box<dyn Reader>,
    column_mapping: HashMap<String, String>, // new_name -> original_name
    filtered_schema: Arc<Schema>,
}

impl ColumnFilterReader {
    /// Create a new ColumnFilterReader
    ///
    /// # Arguments
    /// * `inner` - The inner reader to wrap
    /// * `column_mapping` - Mapping from new column name to original column name
    ///   Example: {"id": "id", "text": "text"} means keep columns "id" and "text" with same names
    pub fn new(
        inner: Box<dyn Reader>,
        column_mapping: HashMap<String, String>,
    ) -> anyhow::Result<Self> {
        // Validate that all mapped columns exist in the schema
        let original_schema = inner.schema();
        for original_name in column_mapping.values() {
            if !original_schema
                .fields()
                .iter()
                .any(|f| f.name() == original_name)
            {
                return Err(anyhow::anyhow!(
                    "Column '{}' not found in source schema. Available columns: {:?}",
                    original_name,
                    original_schema
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                ));
            }
        }

        // Build filtered schema with renamed columns
        let filtered_schema = if column_mapping.is_empty() {
            original_schema.clone()
        } else {
            let mut fields = Vec::new();
            for (new_name, original_name) in &column_mapping {
                if let Some(field) = original_schema
                    .fields()
                    .iter()
                    .find(|f| f.name() == original_name)
                {
                    fields.push(Field::new(
                        new_name.clone(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    ));
                }
            }
            Arc::new(Schema::new(fields))
        };

        Ok(Self {
            inner,
            column_mapping,
            filtered_schema,
        })
    }

    /// Filter and rename columns in a sample
    fn filter_and_rename(&self, sample: Sample) -> Sample {
        if self.column_mapping.is_empty() {
            // No mapping, return as-is
            return sample;
        }

        let mut filtered = Sample::new();

        // Only keep columns that are in the mapping, and rename them
        for (new_name, original_name) in &self.column_mapping {
            if let Some(value) = sample.get(original_name) {
                // Clone the value and set it with the new name
                filtered.set_value(new_name.clone(), value.clone());
            }
        }

        filtered
    }
}

impl Iterator for ColumnFilterReader {
    type Item = anyhow::Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some(Ok(sample)) => Some(Ok(self.filter_and_rename(sample))),
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

impl Reader for ColumnFilterReader {
    fn schema(&self) -> &Arc<Schema> {
        &self.filtered_schema
    }
}
