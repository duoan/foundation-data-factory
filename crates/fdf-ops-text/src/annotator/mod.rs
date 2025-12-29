pub mod special_char_ratio;

use fdf_sdk::OperatorRegistry;

pub fn register(registry: &mut OperatorRegistry) {
    special_char_ratio::register(registry);
}
