use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use rayon::prelude::*;
use std::path::Path;

// Type alias for processed batch result
type ProcessedBatchResult =
    Result<(arrow::record_batch::RecordBatch, usize, Vec<(usize, usize)>), anyhow::Error>;

use crate::config::PipelineConfig;
use crate::io;
use crate::operators;

mod manifest;
pub use manifest::{Manifest, OperatorManifest, StageManifest};

pub fn run_pipeline(config: &PipelineConfig) -> Result<()> {
    println!("Running pipeline: {}", config.name);

    let mut manifest = Manifest::new(config.name.clone());
    let mut previous_output: Option<&crate::config::DataSourceConfig> = None;

    for (stage_idx, stage) in config.stages.iter().enumerate() {
        println!(
            "\n[Stage {}/{}] {}",
            stage_idx + 1,
            config.stages.len(),
            stage.name
        );

        // Determine input source
        let input_source = if let Some(input) = &stage.input {
            &input.source
        } else if let Some(prev) = previous_output {
            prev
        } else {
            anyhow::bail!(
                "Stage '{}' has no input and no previous stage output",
                stage.name
            );
        };

        // Read input
        println!("  Reading input from: {:?}", input_source.path);
        let batches_iter = io::read_data_source(input_source)?;

        // Collect batches to get count for progress bar (we need to know total count)
        // Note: For very large datasets, we might want to estimate or use a different approach
        let batches: Vec<_> = batches_iter.collect::<Result<Vec<_>>>()?;
        let total_batches = batches.len();
        println!("  Read {} batches", total_batches);

        // Create progress bar
        let pb = ProgressBar::new(total_batches as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} batches ({percent}%)",
                )
                .unwrap(),
        );

        // Create operators once
        let mut operator_instances = Vec::new();
        let mut operator_manifests = Vec::new();
        for op_config in &stage.operators {
            let op_name = op_config.get_operator_name();
            let params = op_config.get_params();
            let operator = operators::create_operator(&op_name, params)
                .with_context(|| format!("Failed to create operator: {}", op_name))?;
            let kind = operator.kind().to_string();
            operator_instances.push(operator);
            operator_manifests.push((op_name, kind));
        }

        println!("  Applying {} operators...", stage.operators.len());

        // Create output directory
        let path = Path::new(
            stage
                .output
                .source
                .path
                .as_ref()
                .context("Output path is required")?,
        );
        std::fs::create_dir_all(path)?;

        // Process batches in parallel using Rayon
        // Each batch is processed independently (apply all operators), then results are collected
        let processed_batches: Vec<ProcessedBatchResult> = batches
            .into_par_iter()
            .enumerate()
            .map(|(batch_idx, batch)| {
                let mut batch = batch;
                let initial_input_rows = batch.num_rows();
                let mut batch_operator_stats = vec![(0, 0); operator_instances.len()];

                // Apply all operators sequentially to this batch
                for (op_idx, operator) in operator_instances.iter().enumerate() {
                    let op_input_rows = batch.num_rows();
                    batch = match operator.apply(batch) {
                        Ok(b) => b,
                        Err(e) => {
                            return Err(e.context(format!("Failed to process batch {}", batch_idx)))
                        }
                    };
                    let op_output_rows = batch.num_rows();

                    // Update operator stats for this batch
                    batch_operator_stats[op_idx] = (op_input_rows, op_output_rows);
                }

                // Update progress bar (thread-safe)
                pb.inc(1);

                Ok((batch, initial_input_rows, batch_operator_stats))
            })
            .collect();

        // Collect results and aggregate statistics
        let mut processed_batches_vec = Vec::new();
        let mut total_input_rows = 0;
        let mut total_output_rows = 0;
        let mut operator_stats: Vec<(usize, usize)> = vec![(0, 0); operator_instances.len()];

        for result in processed_batches {
            let (batch, initial_input_rows, batch_stats) = result?;
            total_input_rows += initial_input_rows;
            total_output_rows += batch.num_rows();

            // Aggregate operator stats
            for (op_idx, (op_input, op_output)) in batch_stats.iter().enumerate() {
                operator_stats[op_idx].0 += op_input;
                operator_stats[op_idx].1 += op_output;
            }

            processed_batches_vec.push(batch);
        }

        pb.finish_with_message("All batches processed");

        // Write partitions (sequential to maintain order)
        let partition_size = 10_000; // Rows per partition file
        let mut partition_files = Vec::new();
        let mut current_partition = Vec::new();
        let mut current_partition_rows = 0;
        let mut partition_idx = 0;

        for batch in processed_batches_vec {
            // Add batch to current partition
            if batch.num_rows() > partition_size {
                // Split large batch
                let mut remaining = batch;
                while remaining.num_rows() > partition_size {
                    let split_batch = remaining.slice(0, partition_size);
                    let partition_file = path.join(format!("part-{:05}.parquet", partition_idx));
                    io::write_parquet(vec![split_batch], &partition_file)?;
                    partition_files.push(partition_file.to_string_lossy().to_string());
                    partition_idx += 1;
                    remaining =
                        remaining.slice(partition_size, remaining.num_rows() - partition_size);
                }
                if remaining.num_rows() > 0 {
                    current_partition.push(remaining);
                    current_partition_rows = current_partition.iter().map(|b| b.num_rows()).sum();
                }
            } else if current_partition_rows + batch.num_rows() > partition_size {
                // Write current partition and start new one
                let batch_rows = batch.num_rows();
                let partition_file = path.join(format!("part-{:05}.parquet", partition_idx));
                io::write_parquet(current_partition, &partition_file)?;
                partition_files.push(partition_file.to_string_lossy().to_string());
                current_partition = vec![batch];
                current_partition_rows = batch_rows;
                partition_idx += 1;
            } else {
                // Add to current partition
                let batch_rows = batch.num_rows();
                current_partition.push(batch);
                current_partition_rows += batch_rows;
            }
        }

        // Write remaining partition
        if !current_partition.is_empty() {
            let partition_file = path.join(format!("part-{:05}.parquet", partition_idx));
            io::write_parquet(current_partition, &partition_file)?;
            partition_files.push(partition_file.to_string_lossy().to_string());
        }

        println!("  Total input rows: {}", total_input_rows);

        // Build operator manifests from accumulated stats
        let mut stage_operator_manifests = Vec::new();
        for (op_idx, (op_name, op_kind)) in operator_manifests.iter().enumerate() {
            let (input_rows, output_rows) = operator_stats[op_idx];
            let filtered_rows = if op_kind == "filter" && input_rows > output_rows {
                Some(input_rows - output_rows)
            } else {
                None
            };

            stage_operator_manifests.push(OperatorManifest {
                name: op_name.clone(),
                kind: op_kind.clone(),
                input_rows,
                output_rows,
                filtered_rows,
            });

            println!(
                "    {} ({}): {} -> {} rows{}",
                op_name,
                op_kind,
                input_rows,
                output_rows,
                if let Some(filtered) = filtered_rows {
                    format!(" (filtered: {})", filtered)
                } else {
                    String::new()
                }
            );
        }

        println!("  ✓ Wrote {} partition files", partition_files.len());

        println!(
            "  ✓ Stage completed: {} rows written (from {} input rows)",
            total_output_rows, total_input_rows
        );

        // Create stage manifest
        let stage_manifest = StageManifest {
            name: stage.name.clone(),
            input_path: input_source.path.clone(),
            output_path: path.to_string_lossy().to_string(),
            partition_files,
            operators: stage_operator_manifests,
            total_input_rows,
            total_output_rows,
        };

        manifest.add_stage(stage_manifest);

        // Write manifest for this stage
        let manifest_path = path.join("manifest.json");
        manifest.write_to_file(&manifest_path)?;
        println!("  ✓ Manifest written to: {}", manifest_path.display());

        // Update previous_output for next stage
        previous_output = Some(&stage.output.source);
    }

    println!("\n✓ Pipeline completed successfully!");
    Ok(())
}
