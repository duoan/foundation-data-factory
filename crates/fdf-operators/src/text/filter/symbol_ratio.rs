use fdf_sdk::{Operator, Result, Sample};

pub struct SymbolRatioFilter {
    text_col: String,
    max_symbol_to_word_ratio: f64,
}

impl Operator for SymbolRatioFilter {
    fn process(&self, sample: Sample) -> Result<Option<Sample>> {
        // Get text field
        let text = sample
            .get_str(&self.text_col)
            .ok_or_else(|| anyhow::anyhow!("Missing text field: {}", self.text_col))?;

        // Count symbols using regex
        let symbol_pattern = regex::Regex::new(r"#|\.\.\.|\. \. \.|\u{2026}")?;
        let num_symbols = symbol_pattern.find_iter(text).count();

        // Count words
        let num_words = text.split_whitespace().count().max(1);

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

        Ok(Box::new(SymbolRatioFilter {
            text_col,
            max_symbol_to_word_ratio,
        }))
    });
}
