use anyhow::{Context, Result};
use arrow::array::*;
use arrow::compute::filter_record_batch;
use std::collections::HashMap;

use crate::operators::{FilterBase, Operator, Row};

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
    fn should_keep(&self, row: &Row) -> Result<bool> {
        // Check all metrics - row is kept only if all conditions are satisfied
        for (metric_name, thresholds) in &self.metrics {
            let annotation_col = format!("{}{}", self.annotation_prefix, metric_name);

            // Get value for this metric
            let value = row.get_f64(&annotation_col);

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
        use crate::operators::row::batch_to_rows;
        use rayon::prelude::*;
        
        // Convert batch to rows
        let rows = batch_to_rows(&batch)?;
        
        // Build filter mask by checking each row in parallel
        let keep_mask: Result<Vec<bool>> = rows
            .par_iter()
            .map(|row| <Self as FilterBase>::should_keep(self, row))
            .collect();

        // Convert to BooleanArray and apply filter
        let boolean_array = BooleanArray::from(keep_mask?);
        Ok(filter_record_batch(&batch, &boolean_array)?)
    }
}
