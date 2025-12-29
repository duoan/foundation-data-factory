use fdf_sdk::{Operator, Result, Sample};

pub struct TextLenFilter {
    text_col: String,
    lower_bound: Option<u32>,
    upper_bound: Option<u32>,
}

impl Operator for TextLenFilter {
    fn process(&self, sample: Sample) -> Result<Option<Sample>> {
        // If no bounds specified, keep all records
        if self.lower_bound.is_none() && self.upper_bound.is_none() {
            return Ok(Some(sample));
        }

        // Get text field
        let text = sample
            .get_str(&self.text_col)
            .ok_or_else(|| anyhow::anyhow!("Missing text field: {}", self.text_col))?;

        // Calculate length (character count)
        let len = text.chars().count() as u32;

        // Check bounds
        let lower_ok = self.lower_bound.map(|lb| len >= lb).unwrap_or(true);
        let upper_ok = self.upper_bound.map(|ub| len <= ub).unwrap_or(true);

        if lower_ok && upper_ok {
            Ok(Some(sample))
        } else {
            Ok(None)
        }
    }
}

pub fn register(registry: &mut fdf_sdk::OperatorRegistry) {
    registry.register("text_len_filter", |config: &serde_yaml::Value| {
        let text_col = config["text_col"].as_str().unwrap().to_string();
        let lower_bound = config["lower_bound"].as_u64().map(|v| v as u32);
        let upper_bound = config["upper_bound"].as_u64().map(|v| v as u32);

        Ok(Box::new(TextLenFilter {
            text_col,
            lower_bound,
            upper_bound,
        }))
    });
}
