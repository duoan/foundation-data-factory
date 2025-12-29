pub mod annotator;
pub mod filter;
pub mod transformer;

use fdf_sdk::OperatorRegistry;

pub fn register(registry: &mut OperatorRegistry) {
    filter::register(registry);
    annotator::register(registry);
    // TODO: Register common transformers when implemented
    // transformer::register(registry);
}
