pub mod audio;
pub mod common;
pub mod image;
pub mod text;
pub mod video;

use fdf_sdk::OperatorRegistry;

pub fn register_all(registry: &mut OperatorRegistry) -> Result<(), fdf_sdk::Error> {
    // Register common operators first (available for all modalities)
    common::register(registry);

    // Register modality-specific operators
    text::register(registry);
    // TODO: Register other modalities when implemented
    // image::register(registry);
    // video::register(registry);
    // audio::register(registry);
    Ok(())
}
