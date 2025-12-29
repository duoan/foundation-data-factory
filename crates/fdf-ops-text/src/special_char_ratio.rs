use fdf_sdk::{impl_annotator_operator, BaseAnnotator};
use fdf_sdk::{OperatorRegistry, Result};
use polars::prelude::*;

pub struct SpecialCharRatioAnnotator {
    text_col: String,
    out_col: String,
}

impl BaseAnnotator for SpecialCharRatioAnnotator {
    fn build_annotation(&self) -> Result<(Expr, String)> {
        // No need to clone - col() is cheap, just creates a new Expr reference
        let total_len = col(&self.text_col).str().len_chars();

        // Count alphabetic characters using extract_all to get all matches, then count them
        // extract_all returns a list of all matches, we count the list length
        let alpha_matches = col(&self.text_col).str().extract_all(lit(r"[a-zA-Z]"));
        let alpha_count = alpha_matches.list().len();
        let special_count = (total_len.clone() - alpha_count).fill_null(0);

        // Calculate ratio: special_chars / total_len
        // Polars automatically promotes integer division to Float64, so cast is not needed
        let ratio = when(total_len.clone().gt(lit(0)))
            .then(special_count / total_len)
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
