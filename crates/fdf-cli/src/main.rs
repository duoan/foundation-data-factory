use clap::Parser;
use fdf_engine::PipelineSpec;
use fdf_operators::register_all;
use fdf_sdk::OperatorRegistry;

#[derive(Parser)]
#[command(name = "fdf")]
#[command(about = "Foundation Data Factory - High-performance data pipeline")]
struct Cli {
    #[arg(short, long)]
    config: String,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Load YAML spec
    let spec: PipelineSpec = serde_yaml::from_str(&std::fs::read_to_string(&cli.config)?)?;

    // Register all operators
    let mut registry = OperatorRegistry::new();
    register_all(&mut registry)?;

    // Run pipeline (statistics are printed by run_pipeline)
    fdf_engine::run_pipeline(spec, &registry)?;

    println!("✓ Pipeline completed successfully");
    Ok(())
}
