use fdf_sdk::{Operator, Result, Sample};

pub struct NormalizeTransformer {
    text_col: String,
    lowercase: bool,
    strip: bool,
}

impl Operator for NormalizeTransformer {
    fn process(&self, mut sample: Sample) -> Result<Option<Sample>> {
        // Try to get mutable reference to the string for in-place modification
        if let Some(text_mut) = sample.get_str_mut(&self.text_col) {
            // In-place modification: modify the string directly
            if self.strip && self.lowercase {
                // Both operations: trim first, then lowercase
                let trimmed = text_mut.trim();
                if trimmed.len() != text_mut.len() {
                    // Need to trim - create new string with both operations
                    // Check if ASCII-only for faster path
                    if trimmed.is_ascii() {
                        let mut s = trimmed.to_string();
                        s.make_ascii_lowercase();
                        *text_mut = s;
                    } else {
                        *text_mut = trimmed.to_lowercase();
                    }
                } else {
                    // No trimming needed, just lowercase in place
                    // Use make_ascii_lowercase() for ASCII-only (faster, no allocation)
                    if text_mut.is_ascii() {
                        text_mut.make_ascii_lowercase();
                    } else {
                        *text_mut = text_mut.to_lowercase();
                    }
                }
            } else if self.strip {
                // Only strip: modify in place if needed
                let trimmed = text_mut.trim();
                if trimmed.len() != text_mut.len() {
                    *text_mut = trimmed.to_string();
                }
            } else if self.lowercase {
                // Only lowercase: modify in place
                // Use make_ascii_lowercase() for ASCII-only (faster, no allocation)
                if text_mut.is_ascii() {
                    text_mut.make_ascii_lowercase();
                } else {
                    *text_mut = text_mut.to_lowercase();
                }
            }
            // If neither operation is needed, no modification
        } else {
            // Fallback: value doesn't exist or is not a string
            return Err(anyhow::anyhow!("Missing text field: {}", self.text_col));
        }

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
