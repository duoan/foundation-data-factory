pub mod annotate_const;
pub mod fasttext_classifier_filter;
pub mod filter_leq;
pub mod gopher_quality_filter;
pub mod gopher_repetition_filter;
pub mod normalize;
pub mod special_char_ratio;
pub mod text_len_filter;

use fdf_sdk::{OperatorRegistry, Result};

pub fn register_all(registry: &mut OperatorRegistry) -> Result<()> {
    normalize::register(registry);
    text_len_filter::register(registry);
    special_char_ratio::register(registry);
    gopher_quality_filter::register(registry);
    gopher_repetition_filter::register(registry);
    fasttext_classifier_filter::register(registry);
    filter_leq::register(registry);
    annotate_const::register(registry);
    Ok(())
}
