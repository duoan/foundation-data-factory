use crate::{Result, Sample};

/// Operator trait - unified interface for all operators
/// Returns:
/// - Some(sample) if the sample should be kept (may be modified)
/// - None if the sample should be filtered out
pub trait Operator: Send + Sync {
    fn process(&self, sample: Sample) -> Result<Option<Sample>>;
}

/// Factory for creating operators from config
pub trait OperatorFactory: Send + Sync {
    fn create(&self, config: &serde_yaml::Value) -> Result<Box<dyn Operator>>;
}
