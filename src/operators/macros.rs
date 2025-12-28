/// Macro to automatically implement the Operator trait
///
/// Usage:
/// ```rust
/// impl_operator! {
///     TextStatAnnotator,
///     name: "textstat-annotator",
///     kind: "annotator",
///     apply: |self, batch| {
///         add_textstat_annotations(batch, &self.column)
///     }
/// }
/// ```
#[macro_export]
macro_rules! impl_operator {
    (
        $struct_name:ty,
        name: $name:expr,
        kind: $kind:expr,
        apply: |$self:ident, $batch:ident| $apply_body:block
    ) => {
        impl $crate::operators::Operator for $struct_name {
            fn name(&self) -> &str {
                $name
            }

            fn kind(&self) -> &str {
                $kind
            }

            fn apply(&$self, $batch: arrow::record_batch::RecordBatch) -> anyhow::Result<arrow::record_batch::RecordBatch> {
                $apply_body
            }
        }
    };
}
