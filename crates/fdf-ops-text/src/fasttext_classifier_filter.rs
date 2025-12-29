// Placeholder - will implement later
use fdf_sdk::{impl_filter_operator, BaseFilter};
use fdf_sdk::{OperatorRegistry, Result};
use polars::prelude::*;

pub struct FastTextClassifierFilter {
    #[allow(dead_code)]
    text_col: String,
}

impl BaseFilter for FastTextClassifierFilter {
    fn build_condition(&self) -> Result<Expr> {
        todo!("FastText classifier filter not yet implemented")
    }
}

impl_filter_operator!(FastTextClassifierFilter);

pub fn register(registry: &mut OperatorRegistry) {
    registry.register_fn(
        "text.fasttext_classifier_filter",
        |_config: &serde_yaml::Value| {
            Ok(Box::new(FastTextClassifierFilter {
                text_col: "text".to_string(),
            }))
        },
    );
}
