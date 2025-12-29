use fdf_sdk::BaseFilter;
use fdf_sdk::{Context, Operator, OperatorRegistry, Result};
use polars::prelude::*;

pub struct TextLenFilter {
    text_col: String,
    lower_bound: Option<u32>,
    upper_bound: Option<u32>,
}

impl BaseFilter for TextLenFilter {
    fn build_condition(&self) -> Result<Expr> {
        let len = col(&self.text_col).str().len_chars();
        let mut condition: Option<Expr> = None;

        if let Some(lb) = self.lower_bound {
            condition = Some(len.clone().gt_eq(lb as u64));
        }

        if let Some(ub) = self.upper_bound {
            let ub_cond = len.lt_eq(ub as u64);
            condition = match condition {
                Some(c) => Some(c.and(ub_cond)),
                None => Some(ub_cond),
            };
        }

        // If no condition, return a condition that's always true
        Ok(condition.unwrap_or_else(|| lit(true)))
    }
}

impl Operator for TextLenFilter {
    fn apply(&self, df: LazyFrame, ctx: &Context) -> Result<LazyFrame> {
        // If no bounds specified, condition is always true (lit(true)), so don't filter
        if self.lower_bound.is_none() && self.upper_bound.is_none() {
            return Ok(df);
        }
        self.apply_filter(df, ctx)
    }
}

pub fn register(registry: &mut OperatorRegistry) {
    registry.register_fn("text.len_filter", |config: &serde_yaml::Value| {
        let text_col = config["text_col"].as_str().unwrap().to_string();
        let lower_bound = config["lower_bound"].as_u64().map(|v| v as u32);
        let upper_bound = config["upper_bound"].as_u64().map(|v| v as u32);

        Ok(Box::new(TextLenFilter {
            text_col,
            lower_bound,
            upper_bound,
        }))
    });
}
