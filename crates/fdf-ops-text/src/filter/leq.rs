use fdf_sdk::{impl_filter_operator, BaseFilter};
use fdf_sdk::{OperatorRegistry, Result};
use polars::prelude::*;

pub struct LeqFilter {
    col: String,
    value: f64,
}

impl BaseFilter for LeqFilter {
    fn build_condition(&self) -> Result<Expr> {
        Ok(col(&self.col).lt_eq(self.value))
    }
}

impl_filter_operator!(LeqFilter);

pub fn register(registry: &mut OperatorRegistry) {
    registry.register_fn("filter.leq", |config: &serde_yaml::Value| {
        let col = config["col"].as_str().unwrap().to_string();
        let value = config["value"].as_f64().unwrap();

        Ok(Box::new(LeqFilter { col, value }))
    });
}
