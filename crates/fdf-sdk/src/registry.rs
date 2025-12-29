use crate::op::{Operator, OperatorFactory};
use crate::Result;
use anyhow::anyhow;
use std::collections::HashMap;

#[derive(Default)]
pub struct OperatorRegistry {
    factories: HashMap<String, Box<dyn OperatorFactory>>,
}

impl OperatorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, name: &str, factory: F)
    where
        F: OperatorFactory + 'static,
    {
        self.factories.insert(name.to_string(), Box::new(factory));
    }

    pub fn register_fn<F>(&mut self, name: &str, factory_fn: F)
    where
        F: Fn(&serde_yaml::Value) -> Result<Box<dyn Operator>> + Send + Sync + 'static,
    {
        struct FnFactory<F> {
            f: F,
        }

        impl<F> OperatorFactory for FnFactory<F>
        where
            F: Fn(&serde_yaml::Value) -> Result<Box<dyn Operator>> + Send + Sync,
        {
            fn create(&self, config: &serde_yaml::Value) -> Result<Box<dyn Operator>> {
                (self.f)(config)
            }
        }

        self.factories
            .insert(name.to_string(), Box::new(FnFactory { f: factory_fn }));
    }

    pub fn build(&self, name: &str, config: &serde_yaml::Value) -> Result<Box<dyn Operator>> {
        let factory = self
            .factories
            .get(name)
            .ok_or_else(|| anyhow!("Unknown operator: {}", name))?;
        factory.create(config)
    }
}
