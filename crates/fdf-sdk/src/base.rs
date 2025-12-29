// This file is kept for backward compatibility but is deprecated
// The new architecture uses the Operator trait directly in op.rs
// All operators should implement Operator::process() instead

use crate::{Operator, Result};

/// Base trait for filter operators (deprecated - use Operator trait directly)
/// This is kept for backward compatibility but should not be used in new code
#[deprecated(note = "Use Operator trait directly instead")]
pub trait BaseFilter: Operator {
    /// Build the filter condition expression (deprecated)
    fn build_condition(&self) -> Result<()> {
        // This method is no longer used in the new architecture
        Ok(())
    }
}

/// Base trait for transformer operators (deprecated - use Operator trait directly)
#[deprecated(note = "Use Operator trait directly instead")]
pub trait BaseTransformer: Operator {
    /// Build the transformation expression (deprecated)
    fn build_transformation(&self) -> Result<()> {
        // This method is no longer used in the new architecture
        Ok(())
    }
}

/// Base trait for annotator operators (deprecated - use Operator trait directly)
#[deprecated(note = "Use Operator trait directly instead")]
pub trait BaseAnnotator: Operator {
    /// Build the annotation expression (deprecated)
    fn build_annotation(&self) -> Result<()> {
        // This method is no longer used in the new architecture
        Ok(())
    }
}

// Macros are deprecated - operators should implement Operator trait directly
#[deprecated(note = "Implement Operator trait directly instead")]
#[macro_export]
macro_rules! impl_filter_operator {
    ($type:ty) => {
        // Deprecated - implement Operator trait directly
    };
}

#[deprecated(note = "Implement Operator trait directly instead")]
#[macro_export]
macro_rules! impl_transformer_operator {
    ($type:ty) => {
        // Deprecated - implement Operator trait directly
    };
}

#[deprecated(note = "Implement Operator trait directly instead")]
#[macro_export]
macro_rules! impl_annotator_operator {
    ($type:ty) => {
        // Deprecated - implement Operator trait directly
    };
}
