pub mod annotator;
pub mod filter;
pub mod transformer;

use fdf_sdk::OperatorRegistry;

pub fn register(_registry: &mut OperatorRegistry) {
    // TODO: Register image operators when implemented
    // transformer::register(registry);
    // filter::register(registry);
    // annotator::register(registry);
}
