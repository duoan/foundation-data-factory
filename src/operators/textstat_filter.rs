use anyhow::{Context, Result};
use arrow::array::*;
use arrow::compute::filter_record_batch;
use std::collections::HashMap;

use crate::operators::FilterBase;

#[derive(Debug, Clone)]
struct MetricThresholds {
    min: Option<f64>,
    max: Option<f64>,
}

pub struct TextStatFilter {
    annotation_prefix: String,
    metrics: HashMap<String, MetricThresholds>,
}

impl TextStatFilter {
    pub fn new(params: HashMap<String, serde_yaml::Value>) -> Result<Self> {
        let annotation_prefix = "__annotation_textstat_".to_string();

        let metrics_value = params
            .get("metrics")
            .context("metrics parameter is required for textstat-filter")?;

        let metrics_map = metrics_value
            .as_mapping()
            .context("metrics must be a mapping")?;

        let mut metrics = HashMap::new();
        for (key, value) in metrics_map {
            let metric_name = key
                .as_str()
                .context("metric name must be a string")?
                .to_string();
            let thresholds: &serde_yaml::Mapping = value
                .as_mapping()
                .context("metric thresholds must be a mapping")?;

            let min = thresholds
                .get(serde_yaml::Value::String("min".to_string()))
                .and_then(|v: &serde_yaml::Value| v.as_f64());
            let max = thresholds
                .get(serde_yaml::Value::String("max".to_string()))
                .and_then(|v: &serde_yaml::Value| v.as_f64());

            metrics.insert(metric_name, MetricThresholds { min, max });
        }

        Ok(Self {
            annotation_prefix,
            metrics,
        })
    }
}

impl FilterBase for TextStatFilter {
    fn should_keep(
        &self,
        batch: &arrow::record_batch::RecordBatch,
        row_idx: usize,
    ) -> Result<bool> {
        // Check all metrics - row is kept only if all conditions are satisfied
        for (metric_name, thresholds) in &self.metrics {
            let annotation_col = format!("{}{}", self.annotation_prefix, metric_name);

            // Find column index
            let col_idx = batch
                .schema()
                .fields()
                .iter()
                .position(|f| f.name().as_str() == annotation_col.as_str())
                .context(format!("Column {} not found", annotation_col))?;

            let col = batch.column(col_idx);
            let float_array = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("Column is not Float64")?;

            // Get value for this row (returns None if null)
            let value = if float_array.is_valid(row_idx) {
                Some(float_array.value(row_idx))
            } else {
                None
            };

            // Check thresholds
            // If value is None/null, we skip this condition (treat as passing)
            if let Some(val) = value {
                if let Some(min) = thresholds.min {
                    if val < min {
                        return Ok(false);
                    }
                }
                if let Some(max) = thresholds.max {
                    if val > max {
                        return Ok(false);
                    }
                }
            }
            // If value is None, we skip this condition (treat as passing)
        }

        Ok(true)
    }
}

impl_operator! {
    TextStatFilter,
    name: "textstat-filter",
    kind: "filter",
    apply: |self, batch| {
        // Build filter mask by checking each row
        let num_rows = batch.num_rows();
        let mut keep_mask = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let should_keep = <Self as FilterBase>::should_keep(self, &batch, row_idx)?;
            keep_mask.push(should_keep);
        }

        // Convert to BooleanArray and apply filter
        let boolean_array = BooleanArray::from(keep_mask);
        Ok(filter_record_batch(&batch, &boolean_array)?)
    }
}
