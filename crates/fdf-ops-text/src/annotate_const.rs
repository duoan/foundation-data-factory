use fdf_sdk::{impl_annotator_operator, BaseAnnotator};
use fdf_sdk::{OperatorRegistry, Result};
use polars::prelude::*;

pub struct ConstAnnotator {
    col: String,
    value: String,
}

impl BaseAnnotator for ConstAnnotator {
    fn build_annotation(&self) -> Result<(Expr, String)> {
        Ok((lit(self.value.clone()), self.col.clone()))
    }
}

impl_annotator_operator!(ConstAnnotator);

pub fn register(registry: &mut OperatorRegistry) {
    registry.register_fn("annotate.const", |config: &serde_yaml::Value| {
        let col = config["col"].as_str().unwrap().to_string();
        let value = config["value"].as_str().unwrap().to_string();

        Ok(Box::new(ConstAnnotator { col, value }))
    });
}
