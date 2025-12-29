pub mod annotator;
pub mod filter;
pub mod transformer;

use fdf_sdk::OperatorRegistry;

pub fn register(registry: &mut OperatorRegistry) {
    transformer::register(registry);
    filter::register(registry);
    annotator::register(registry);
}
