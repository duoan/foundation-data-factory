use fdf_sdk::{impl_transformer_operator, BaseTransformer};
use fdf_sdk::{OperatorRegistry, Result};
use polars::prelude::*;

pub struct NormalizeTransformer {
    text_col: String,
    lowercase: bool,
    strip: bool,
}

impl BaseTransformer for NormalizeTransformer {
    fn build_transformation(&self) -> Result<(Expr, String, bool)> {
        let mut expr = col(&self.text_col);

        if self.strip {
            expr = expr.str().strip_chars(lit(""));
        }

        if self.lowercase {
            expr = expr.str().to_lowercase();
        }

        // Always in-place: modify the original column
        Ok((expr, self.text_col.clone(), true))
    }
}

impl_transformer_operator!(NormalizeTransformer);

pub fn register(registry: &mut OperatorRegistry) {
    registry.register_fn("text.normalize", |config: &serde_yaml::Value| {
        let text_col = config["text_col"].as_str().unwrap().to_string();
        let lowercase = config["lowercase"].as_bool().unwrap_or(false);
        let strip = config["strip"].as_bool().unwrap_or(false);

        Ok(Box::new(NormalizeTransformer {
            text_col,
            lowercase,
            strip,
        }))
    });
}
