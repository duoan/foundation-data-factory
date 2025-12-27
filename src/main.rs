use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod config;
mod io;
mod operators;
mod runtime;

use config::PipelineConfig;

#[derive(Parser)]
#[command(name = "fdf")]
#[command(about = "FoundationDataFactory - High-performance data processing pipeline", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a pipeline from YAML configuration
    Run {
        /// Path to pipeline YAML file
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Validate a pipeline configuration
    Validate {
        /// Path to pipeline YAML file
        #[arg(short, long)]
        config: PathBuf,
    },
    /// Show version information
    Version,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { config } => {
            let pipeline = PipelineConfig::from_yaml_file(&config)?;
            runtime::run_pipeline(&pipeline)?;
        }
        Commands::Validate { config } => {
            let _pipeline = PipelineConfig::from_yaml_file(&config)?;
            println!("✓ Pipeline configuration is valid");
        }
        Commands::Version => {
            println!("fdf version {}", env!("CARGO_PKG_VERSION"));
        }
    }

    Ok(())
}
