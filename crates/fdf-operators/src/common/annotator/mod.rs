mod add_id;

use fdf_sdk::OperatorRegistry;

pub fn register(registry: &mut OperatorRegistry) {
    add_id::register(registry);
}
