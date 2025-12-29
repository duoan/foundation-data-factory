use fdf_sdk::{impl_annotator_operator, BaseAnnotator};
use fdf_sdk::{OperatorRegistry, Result};
use polars::prelude::*;

pub struct SpecialCharRatioAnnotator {
    text_col: String,
    out_col: String,
}

impl BaseAnnotator for SpecialCharRatioAnnotator {
    fn build_annotation(&self) -> Result<(Expr, String)> {
        let text_col = col(&self.text_col);
        let total_len = text_col.clone().str().len_chars();

        // Count non-alphabetic characters directly using count_matches with inverted pattern
        // Pattern [^a-zA-Z] matches non-alphabetic characters
        // Second parameter false means the pattern is a regex, not a literal string
        let special_count = text_col
            .clone()
            .str()
            .count_matches(lit(r"[^a-zA-Z]"), false)
            .fill_null(0);

        // Calculate ratio: special_chars / total_len
        let ratio = when(total_len.clone().gt(lit(0)))
            .then(special_count.cast(DataType::Float64) / total_len.cast(DataType::Float64))
            .otherwise(lit(0.0))
            .fill_null(lit(0.0));

        Ok((ratio, self.out_col.clone()))
    }
}

impl_annotator_operator!(SpecialCharRatioAnnotator);

pub fn register(registry: &mut OperatorRegistry) {
    registry.register_fn("text.special_char_ratio", |config: &serde_yaml::Value| {
        let text_col = config["text_col"].as_str().unwrap().to_string();
        let out_col = config["out_col"].as_str().unwrap().to_string();

        Ok(Box::new(SpecialCharRatioAnnotator { text_col, out_col }))
    });
}
