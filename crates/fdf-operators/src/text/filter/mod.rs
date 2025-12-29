pub mod fasttext_classifier;
pub mod gopher_quality;
pub mod gopher_repetition;
pub mod leq;
pub mod symbol_ratio;
pub mod text_len;

use fdf_sdk::OperatorRegistry;

pub fn register(registry: &mut OperatorRegistry) {
    text_len::register(registry);
    symbol_ratio::register(registry);
    leq::register(registry);
    gopher_quality::register(registry);
    gopher_repetition::register(registry);
    fasttext_classifier::register(registry);
}
