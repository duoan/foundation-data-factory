pub mod base;
pub mod context;
pub mod op;
pub mod registry;

pub use base::{BaseAnnotator, BaseFilter};
pub use context::Context;
pub use op::{Operator, OperatorFactory};
pub use registry::OperatorRegistry;

// Re-export anyhow for convenience
pub use anyhow::{Error, Result};
