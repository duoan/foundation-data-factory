use crate::spec::PipelineSpec;
use fdf_sdk::{Context, Operator, OperatorRegistry, Result};
use polars::prelude::*;

pub struct Plan {
    ops: Vec<(String, Box<dyn Operator>)>,
    source_path: String,
    sink_path: String,
}

impl Plan {
    pub fn compile(spec: PipelineSpec, registry: &OperatorRegistry) -> Result<Self> {
        let mut ops = Vec::new();

        for op_node in spec.pipeline {
            let op: Box<dyn Operator> = registry.build(&op_node.op, &op_node.config)?;
            ops.push((op_node.op, op));
        }

        Ok(Self {
            ops,
            source_path: spec.source.uris[0].clone(),
            sink_path: spec.sink.uri,
        })
    }

    pub fn execute(&self) -> Result<()> {
        let mut df = LazyFrame::scan_parquet(&self.source_path, ScanArgsParquet::default())?;
        let ctx = Context::default();

        for (_name, op) in &self.ops {
            df = op.apply(df, &ctx)?;
        }

        // Create output directory
        if let Some(parent) = std::path::Path::new(&self.sink_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Write parquet
        let df: DataFrame = df.collect()?;
        let mut file: std::fs::File = std::fs::File::create(&self.sink_path)?;
        ParquetWriter::new(&mut file).finish(&mut df.clone())?;
        Ok(())
    }
}
