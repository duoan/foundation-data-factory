use crate::{Operator, OperatorFactory, Result};
use serde_yaml::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Registry for operators
#[derive(Default)]
pub struct OperatorRegistry {
    factories: HashMap<String, Arc<dyn OperatorFactory>>,
}

impl OperatorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, name: &str, factory: F)
    where
        F: Fn(&Value) -> Result<Box<dyn Operator>> + Send + Sync + 'static,
    {
        struct FactoryFn<F>(F);
        impl<F> OperatorFactory for FactoryFn<F>
        where
            F: Fn(&Value) -> Result<Box<dyn Operator>> + Send + Sync,
        {
            fn create(&self, config: &Value) -> Result<Box<dyn Operator>> {
                (self.0)(config)
            }
        }

        self.factories
            .insert(name.to_string(), Arc::new(FactoryFn(factory)));
    }

    pub fn build(&self, name: &str, config: &Value) -> Result<Box<dyn Operator>> {
        let factory = self
            .factories
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("Unknown operator: {}", name))?;
        factory.create(config)
    }
}
