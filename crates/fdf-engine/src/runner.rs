use crate::plan::Plan;
use crate::spec::PipelineSpec;
use fdf_sdk::{OperatorRegistry, Result};

pub fn run_pipeline(spec: PipelineSpec, registry: &OperatorRegistry) -> Result<()> {
    let plan = Plan::compile(spec, registry)?;
    plan.execute()
}
