pub mod annotator;
pub mod filter;
pub mod transformer;

use fdf_sdk::{OperatorRegistry, Result};

pub fn register_all(registry: &mut OperatorRegistry) -> Result<()> {
    transformer::register(registry);
    filter::register(registry);
    annotator::register(registry);
    Ok(())
}
