use anyhow::{Context, Result};
use arrow::array::{BooleanArray, Float64Array};
use arrow::record_batch::RecordBatch;

use crate::operators::{Operator, where_filter};

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

impl Operator for TextStatFilter {
    fn name(&self) -> &str {
        "textstat-filter"
    }

    fn kind(&self) -> &str {
        "filter"
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let schema = batch.schema();
        let mut mask: Option<BooleanArray> = None;

        // Build filter conditions (like DataFrame.where)
        for (metric_name, thresholds) in &self.metrics {
            let annotation_col = format!("{}{}", self.annotation_prefix, metric_name);

            let col_idx = schema
                .fields()
                .iter()
                .position(|f| f.name().as_str() == annotation_col.as_str())
                .context(format!("Column {} not found", annotation_col))?;

            let float_array = batch
                .column(col_idx)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("Column is not Float64")?;

            // Build condition
            let cond = BooleanArray::from_iter(float_array.iter().map(|v| -> Option<bool> {
                match v {
                    Some(val) => {
                        let min_ok = thresholds.min.is_none_or(|min| val >= min);
                        let max_ok = thresholds.max.is_none_or(|max| val <= max);
                        Some(min_ok && max_ok)
                    }
                    None => Some(true), // NULL values pass
                }
            }));

            // Combine conditions (AND)
            if let Some(existing) = mask {
                let combined: Vec<Option<bool>> = existing
                    .iter()
                    .zip(cond.iter())
                    .map(|(e, c)| match (e, c) {
                        (Some(true), Some(true)) => Some(true),
                        _ => Some(false),
                    })
                    .collect();
                mask = Some(BooleanArray::from_iter(combined));
            } else {
                mask = Some(cond);
            }
        }

        // Apply filter (like DataFrame.where)
        let final_mask = mask.unwrap_or_else(|| {
            BooleanArray::from(vec![true; batch.num_rows()])
        });
        where_filter(batch, &final_mask)
    }
}
