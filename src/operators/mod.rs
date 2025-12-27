use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

pub mod textstat;

pub trait Operator: Send + Sync {
    fn name(&self) -> &str;
    fn kind(&self) -> &str;
    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch>;
}

pub struct TextStatAnnotator {
    column: String,
}

impl TextStatAnnotator {
    pub fn new(params: HashMap<String, serde_yaml::Value>) -> Self {
        let column = params
            .get("column")
            .and_then(|v| v.as_str())
            .unwrap_or("text")
            .to_string();
        Self { column }
    }
}

impl Operator for TextStatAnnotator {
    fn name(&self) -> &str {
        "textstat-annotator"
    }

    fn kind(&self) -> &str {
        "annotator"
    }

    fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        textstat::add_textstat_annotations(batch, &self.column)
    }
}

pub struct TextStatFilter {
    _column: String, // Reserved for future use
    annotation_prefix: String,
    metrics: HashMap<String, MetricThresholds>,
}

#[derive(Debug, Clone)]
struct MetricThresholds {
    min: Option<f64>,
    max: Option<f64>,
}

impl TextStatFilter {
    pub fn new(params: HashMap<String, serde_yaml::Value>) -> Result<Self> {
        let _column = params
            .get("column")
            .and_then(|v| v.as_str())
            .unwrap_or("text")
            .to_string();

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
                .get(&serde_yaml::Value::String("min".to_string()))
                .and_then(|v: &serde_yaml::Value| v.as_f64());
            let max = thresholds
                .get(&serde_yaml::Value::String("max".to_string()))
                .and_then(|v: &serde_yaml::Value| v.as_f64());

            metrics.insert(metric_name, MetricThresholds { min, max });
        }

        Ok(Self {
            _column,
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
        use arrow::array::*;
        use arrow::compute::filter_record_batch;

        // Build filter mask
        let mut mask: Option<BooleanArray> = None;

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

            // Build condition for this metric
            // Note: If the annotation value is None/null, we skip the filter for that row
            // (treat it as passing the filter condition)
            let cond = BooleanArray::from_iter(float_array.iter().map(|v| -> Option<bool> {
                match v {
                    Some(val) => {
                        let min_ok = thresholds.min.map_or(true, |min| val >= min);
                        let max_ok = thresholds.max.map_or(true, |max| val <= max);
                        Some(min_ok && max_ok)
                    }
                    None => Some(true), // Null values pass the filter (skip this condition)
                }
            }));

            // Combine with previous conditions (AND)
            if let Some(existing) = mask {
                mask = Some(arrow::compute::and(&existing, &cond)?);
            } else {
                mask = Some(cond);
            }
        }

        // Apply filter
        if let Some(mask) = mask {
            Ok(filter_record_batch(&batch, &mask)?)
        } else {
            Ok(batch)
        }
    }
}

pub fn create_operator(
    name: &str,
    params: HashMap<String, serde_yaml::Value>,
) -> Result<Box<dyn Operator>> {
    match name {
        "textstat-annotator" => Ok(Box::new(TextStatAnnotator::new(params))),
        "textstat-filter" => Ok(Box::new(TextStatFilter::new(params)?)),
        _ => anyhow::bail!("Unknown operator: {}", name),
    }
}
