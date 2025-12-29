pub mod numeric_range_filter;

use fdf_sdk::OperatorRegistry;

pub fn register(registry: &mut OperatorRegistry) {
    numeric_range_filter::register(registry);
}
