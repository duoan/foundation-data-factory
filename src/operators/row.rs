use anyhow::Result;
use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

/// A single row/sample from a RecordBatch
/// Provides a simple interface to access column values without dealing with Arrow API
#[derive(Debug, Clone)]
pub struct Row {
    pub(crate) values: HashMap<String, Value>,
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Float64(f64),
    Int64(i64),
    Bool(bool),
    Null,
}

impl Row {
    /// Get a string value from the row
    pub fn get_string(&self, column: &str) -> Option<&str> {
        match self.values.get(column)? {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get a float64 value from the row
    pub fn get_f64(&self, column: &str) -> Option<f64> {
        match self.values.get(column)? {
            Value::Float64(f) => Some(*f),
            _ => None,
        }
    }

    /// Get an int64 value from the row
    pub fn get_i64(&self, column: &str) -> Option<i64> {
        match self.values.get(column)? {
            Value::Int64(i) => Some(*i),
            _ => None,
        }
    }

    /// Get a bool value from the row
    pub fn get_bool(&self, column: &str) -> Option<bool> {
        match self.values.get(column)? {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Check if a column exists and is not null
    pub fn has(&self, column: &str) -> bool {
        self.values.contains_key(column)
            && !matches!(self.values.get(column), Some(Value::Null))
    }
}

/// Convert a RecordBatch to a vector of Rows
pub fn batch_to_rows(batch: &RecordBatch) -> Result<Vec<Row>> {
    let num_rows = batch.num_rows();
    let num_cols = batch.num_columns();
    let schema = batch.schema();

    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut values = HashMap::new();

        for col_idx in 0..num_cols {
            let field = schema.field(col_idx);
            let column = batch.column(col_idx);
            let value = extract_value(column, row_idx, field.data_type())?;
            values.insert(field.name().clone(), value);
        }

        rows.push(Row { values });
    }

    Ok(rows)
}

/// Convert a vector of Rows back to a RecordBatch
/// Note: This builds a new schema from the rows (includes all columns in the rows)
pub fn rows_to_batch(rows: &[Row], original_schema: &Schema) -> Result<RecordBatch> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(original_schema.clone())));
    }

    // Get all column names from the first row (rows should have the same columns)
    let first_row = &rows[0];
    let mut column_names: Vec<String> = first_row.values.keys().cloned().collect();
    column_names.sort(); // Sort for consistent ordering

    // Build new schema and columns
    let mut fields = Vec::new();
    let mut columns = Vec::new();

    // First, add all original columns (preserve order)
    for field in original_schema.fields() {
        if first_row.values.contains_key(field.name()) {
            let array = build_array(rows, field.name(), field.data_type())?;
            fields.push(field.clone());
            columns.push(array);
        }
    }

    // Then add new columns (annotation columns)
    for col_name in &column_names {
        if original_schema.field_with_name(col_name).is_err() {
            // This is a new column, assume Float64 for annotation columns
            let field = Field::new(col_name, DataType::Float64, true);
            let array = build_array(rows, col_name, &DataType::Float64)?;
            fields.push(field);
            columns.push(array);
        }
    }

    let new_schema = Schema::new(fields);
    RecordBatch::try_new(Arc::new(new_schema), columns)
        .map_err(|e| anyhow::anyhow!("Failed to create RecordBatch: {}", e))
}

/// Extract a value from an Arrow array at a specific row index
fn extract_value(array: &Arc<dyn arrow::array::Array>, row_idx: usize, data_type: &DataType) -> Result<Value> {
    if !array.is_valid(row_idx) {
        return Ok(Value::Null);
    }

    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let string_array = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected StringArray"))?;
            Ok(Value::String(string_array.value(row_idx).to_string()))
        }
        DataType::Float64 => {
            let float_array = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Float64Array"))?;
            Ok(Value::Float64(float_array.value(row_idx)))
        }
        DataType::Int64 => {
            let int_array = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow::anyhow!("Expected Int64Array"))?;
            Ok(Value::Int64(int_array.value(row_idx)))
        }
        DataType::Boolean => {
            let bool_array = array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected BooleanArray"))?;
            Ok(Value::Bool(bool_array.value(row_idx)))
        }
        _ => {
            // For unsupported types, return Null
            Ok(Value::Null)
        }
    }
}

/// Build an Arrow array from rows for a specific column
fn build_array(rows: &[Row], field_name: &str, data_type: &DataType) -> Result<Arc<dyn arrow::array::Array>> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => {
            let values: Vec<Option<String>> = rows.iter()
                .map(|row| {
                    row.get_string(field_name).map(|s| s.to_string())
                })
                .collect();
            Ok(Arc::new(StringArray::from_iter(values)))
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = rows.iter()
                .map(|row| row.get_f64(field_name))
                .collect();
            Ok(Arc::new(Float64Array::from_iter(values)))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = rows.iter()
                .map(|row| row.get_i64(field_name))
                .collect();
            Ok(Arc::new(Int64Array::from_iter(values)))
        }
        DataType::Boolean => {
            let values: Vec<Option<bool>> = rows.iter()
                .map(|row| row.get_bool(field_name))
                .collect();
            Ok(Arc::new(BooleanArray::from_iter(values)))
        }
        _ => {
            anyhow::bail!("Unsupported data type: {:?}", data_type)
        }
    }
}
