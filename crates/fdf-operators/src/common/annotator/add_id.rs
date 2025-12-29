use fdf_sdk::{Operator, Result, Sample};

pub struct AddIdAnnotator {
    id_col: String,
}

impl Operator for AddIdAnnotator {
    fn process(&self, mut sample: Sample) -> Result<Option<Sample>> {
        // Generate UUID4
        let id = uuid::Uuid::new_v4().to_string();
        sample.set_str(&self.id_col, id);
        Ok(Some(sample)) // Keep the sample
    }
}

pub fn register(registry: &mut fdf_sdk::OperatorRegistry) {
    registry.register("add_id", |config: &serde_yaml::Value| {
        let id_col = config["id_col"].as_str().unwrap_or("id").to_string();

        Ok(Box::new(AddIdAnnotator { id_col }))
    });
}
