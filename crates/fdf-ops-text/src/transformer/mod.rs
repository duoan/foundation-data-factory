pub mod normalize;

use fdf_sdk::OperatorRegistry;

pub fn register(registry: &mut OperatorRegistry) {
    normalize::register(registry);
}
