use fdf_sdk::{Operator, Result, Sample};

pub struct NormalizeTransformer {
    text_col: String,
    lowercase: bool,
    strip: bool,
}

impl Operator for NormalizeTransformer {
    fn process(&self, mut sample: Sample) -> Result<Option<Sample>> {
        // Get text field
        let text = sample
            .get_str(&self.text_col)
            .ok_or_else(|| anyhow::anyhow!("Missing text field: {}", self.text_col))?;

        // Apply transformations
        let mut transformed = text.to_string();

        if self.strip {
            transformed = transformed.trim().to_string();
        }

        if self.lowercase {
            transformed = transformed.to_lowercase();
        }

        // Write back to the same column
        sample.set_str(&self.text_col, transformed);

        Ok(Some(sample)) // Keep the sample
    }
}

pub fn register(registry: &mut fdf_sdk::OperatorRegistry) {
    registry.register(
        "text_normalize_transformer",
        |config: &serde_yaml::Value| {
            let text_col = config["text_col"].as_str().unwrap().to_string();
            let lowercase = config["lowercase"].as_bool().unwrap_or(false);
            let strip = config["strip"].as_bool().unwrap_or(false);

            Ok(Box::new(NormalizeTransformer {
                text_col,
                lowercase,
                strip,
            }))
        },
    );
}
