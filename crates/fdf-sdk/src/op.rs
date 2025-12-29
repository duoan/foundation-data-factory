use crate::context::Context;
use crate::Result;
use polars::prelude::*;

pub trait Operator: Send + Sync {
    fn apply(&self, df: LazyFrame, ctx: &Context) -> Result<LazyFrame>;
}

pub trait OperatorFactory: Send + Sync {
    fn create(&self, config: &serde_yaml::Value) -> Result<Box<dyn Operator>>;
}
