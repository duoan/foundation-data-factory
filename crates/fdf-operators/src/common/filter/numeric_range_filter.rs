use fdf_sdk::{Operator, Result, Sample};

pub struct NumericRangeFilter {
    col: String,
    lower_bound: Option<f64>,
    upper_bound: Option<f64>,
    negate: bool,
}

impl Operator for NumericRangeFilter {
    fn process(&self, sample: Sample) -> Result<Option<Sample>> {
        // Get numeric value
        let value = sample
            .get_f64(&self.col)
            .ok_or_else(|| anyhow::anyhow!("Missing numeric field: {}", self.col))?;

        // Check bounds
        let lower_ok = self.lower_bound.map(|lb| value >= lb).unwrap_or(true);
        let upper_ok = self.upper_bound.map(|ub| value <= ub).unwrap_or(true);
        let in_range = lower_ok && upper_ok;

        // Apply negation if needed
        if (self.negate && !in_range) || (!self.negate && in_range) {
            Ok(Some(sample))
        } else {
            Ok(None)
        }
    }
}

pub fn register(registry: &mut fdf_sdk::OperatorRegistry) {
    registry.register("numeric_range_filter", |config: &serde_yaml::Value| {
        let col = config["col"].as_str().unwrap().to_string();
        let lower_bound = config["lower_bound"].as_f64();
        let upper_bound = config["upper_bound"].as_f64();
        let negate = config["negate"].as_bool().unwrap_or(false);

        Ok(Box::new(NumericRangeFilter {
            col,
            lower_bound,
            upper_bound,
            negate,
        }))
    });
}
