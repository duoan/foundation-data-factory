use fdf_sdk::{Operator, Result, Sample};
use regex::Regex;

pub struct SymbolRatioFilter {
    text_col: String,
    max_symbol_to_word_ratio: f64,
    symbol_pattern: Regex, // Pre-compiled regex for better performance
}

impl SymbolRatioFilter {
    /// Create a new SymbolRatioFilter with pre-compiled regex
    pub fn new(text_col: String, max_symbol_to_word_ratio: f64) -> Result<Self> {
        // Compile regex once during initialization
        let symbol_pattern = Regex::new(r"#|\.\.\.|\. \. \.|\u{2026}")?;
        Ok(Self {
            text_col,
            max_symbol_to_word_ratio,
            symbol_pattern,
        })
    }
}

impl Operator for SymbolRatioFilter {
    fn process(&self, sample: Sample) -> Result<Option<Sample>> {
        // Get text field
        let text = sample
            .get_str(&self.text_col)
            .ok_or_else(|| anyhow::anyhow!("Missing text field: {}", self.text_col))?;

        // Count symbols using pre-compiled regex (much faster)
        let num_symbols = self.symbol_pattern.find_iter(text).count();

        // Count words efficiently using byte-based iteration for better performance
        // This avoids the overhead of char iteration and is faster for ASCII text
        let num_words = if text.is_empty() {
            1
        } else {
            // Use byte-based counting for ASCII text (most common case)
            // This is faster than char-based iteration
            let bytes = text.as_bytes();
            let mut word_count = 0;
            let mut in_word = false;

            for &byte in bytes {
                let is_whitespace = byte == b' ' || byte == b'\t' || byte == b'\n' || byte == b'\r';
                if is_whitespace {
                    if in_word {
                        in_word = false;
                    }
                } else if !in_word {
                    word_count += 1;
                    in_word = true;
                }
            }
            word_count.max(1)
        };

        // Calculate ratio
        let ratio = num_symbols as f64 / num_words as f64;

        // Filter: keep rows where ratio <= max_symbol_to_word_ratio
        if ratio <= self.max_symbol_to_word_ratio {
            Ok(Some(sample))
        } else {
            Ok(None)
        }
    }
}

pub fn register(registry: &mut fdf_sdk::OperatorRegistry) {
    registry.register("text_symbol_ratio_filter", |config: &serde_yaml::Value| {
        let text_col = config["text_col"].as_str().unwrap_or("text").to_string();
        let max_symbol_to_word_ratio = config["max_symbol_to_word_ratio"]
            .as_f64()
            .unwrap_or(f64::MAX);

        Ok(Box::new(SymbolRatioFilter::new(
            text_col,
            max_symbol_to_word_ratio,
        )?))
    });
}
