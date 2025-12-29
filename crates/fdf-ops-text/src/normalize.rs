use fdf_sdk::{impl_annotator_operator, BaseAnnotator};
use fdf_sdk::{OperatorRegistry, Result};
use polars::prelude::*;

pub struct NormalizeAnnotator {
    text_col: String,
    out_col: String,
    lowercase: bool,
    strip: bool,
}

impl BaseAnnotator for NormalizeAnnotator {
    fn build_annotation(&self) -> Result<(Expr, String)> {
        let mut expr = col(&self.text_col);

        if self.strip {
            expr = expr.str().strip_chars(lit(""));
        }

        if self.lowercase {
            expr = expr.str().to_lowercase();
        }

        Ok((expr, self.out_col.clone()))
    }
}

impl_annotator_operator!(NormalizeAnnotator);

pub fn register(registry: &mut OperatorRegistry) {
    registry.register_fn("text.normalize", |config: &serde_yaml::Value| {
        let text_col = config["text_col"].as_str().unwrap().to_string();
        let out_col = config["out_col"].as_str().unwrap().to_string();
        let lowercase = config["lowercase"].as_bool().unwrap_or(false);
        let strip = config["strip"].as_bool().unwrap_or(false);

        Ok(Box::new(NormalizeAnnotator {
            text_col,
            out_col,
            lowercase,
            strip,
        }))
    });
}
