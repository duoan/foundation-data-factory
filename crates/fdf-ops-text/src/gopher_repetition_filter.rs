// Placeholder - will implement later
use fdf_sdk::{impl_filter_operator, BaseFilter};
use fdf_sdk::{OperatorRegistry, Result};
use polars::prelude::*;

pub struct GopherRepetitionFilter {
    #[allow(dead_code)]
    text_col: String,
}

impl BaseFilter for GopherRepetitionFilter {
    fn build_condition(&self) -> Result<Expr> {
        todo!("Gopher repetition filter not yet implemented")
    }
}

impl_filter_operator!(GopherRepetitionFilter);

pub fn register(registry: &mut OperatorRegistry) {
    registry.register_fn(
        "text.gopher_repetition_filter",
        |_config: &serde_yaml::Value| {
            Ok(Box::new(GopherRepetitionFilter {
                text_col: "text".to_string(),
            }))
        },
    );
}
