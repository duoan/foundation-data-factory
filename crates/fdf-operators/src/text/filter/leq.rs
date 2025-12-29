use fdf_sdk::{Operator, Result, Sample};

pub struct LeqFilter {
    col: String,
    value: f64,
}

impl Operator for LeqFilter {
    fn process(&self, sample: Sample) -> Result<Option<Sample>> {
        let val = sample
            .get_f64(&self.col)
            .ok_or_else(|| anyhow::anyhow!("Missing numeric field: {}", self.col))?;

        if val <= self.value {
            Ok(Some(sample))
        } else {
            Ok(None)
        }
    }
}

pub fn register(registry: &mut fdf_sdk::OperatorRegistry) {
    registry.register("filter_leq", |config: &serde_yaml::Value| {
        let col = config["col"].as_str().unwrap().to_string();
        let value = config["value"].as_f64().unwrap();

        Ok(Box::new(LeqFilter { col, value }))
    });
}
