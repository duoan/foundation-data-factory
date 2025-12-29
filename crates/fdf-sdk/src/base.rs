use crate::{Context, Operator, Result};
use polars::prelude::*;

/// Base trait for filter operators
/// Filters reduce the number of rows based on a condition
pub trait BaseFilter: Operator {
    /// Build the filter condition expression
    fn build_condition(&self) -> Result<Expr>;

    /// Helper method to apply filter
    fn apply_filter(&self, df: LazyFrame, _ctx: &Context) -> Result<LazyFrame> {
        let condition = self.build_condition()?;
        Ok(df.filter(condition))
    }
}

/// Base trait for annotator operators
/// Annotators add new columns without changing row count
pub trait BaseAnnotator: Operator {
    /// Build the annotation expression and output column name
    fn build_annotation(&self) -> Result<(Expr, String)>;

    /// Helper method to apply annotation
    fn apply_annotation(&self, df: LazyFrame, _ctx: &Context) -> Result<LazyFrame> {
        let (expr, col_name) = self.build_annotation()?;
        Ok(df.with_columns([expr.alias(&col_name)]))
    }
}

/// Macro to automatically implement Operator for BaseFilter types
/// Usage: impl_filter_operator!(MyFilter);
#[macro_export]
macro_rules! impl_filter_operator {
    ($type:ty) => {
        impl $crate::Operator for $type {
            fn apply(
                &self,
                df: polars::prelude::LazyFrame,
                ctx: &$crate::Context,
            ) -> $crate::Result<polars::prelude::LazyFrame> {
                <$type as $crate::base::BaseFilter>::apply_filter(self, df, ctx)
            }
        }
    };
}

/// Macro to automatically implement Operator for BaseAnnotator types
/// Usage: impl_annotator_operator!(MyAnnotator);
#[macro_export]
macro_rules! impl_annotator_operator {
    ($type:ty) => {
        impl $crate::Operator for $type {
            fn apply(
                &self,
                df: polars::prelude::LazyFrame,
                ctx: &$crate::Context,
            ) -> $crate::Result<polars::prelude::LazyFrame> {
                <$type as $crate::base::BaseAnnotator>::apply_annotation(self, df, ctx)
            }
        }
    };
}

