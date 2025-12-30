use crate::io::{ReaderFactory, Writer, WriterFactory};
use crate::spec::PipelineSpec;
use fdf_sdk::{Operator, OperatorRegistry, Result, Sample};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::path::Path;

pub struct Plan {
    operators: Vec<(String, Box<dyn Operator>)>,
    spec: PipelineSpec,
}

pub struct ProcessingStatistics {
    pub num_documents: usize,
    pub step_statistics: Vec<StepStatistics>,
    pub read_time_ms: u64,
    pub write_time_ms: u64,
}

pub struct StepStatistics {
    pub step_name: String,
    pub step_index: usize,
    pub processing_time_ms: u64,
    pub documents_removed: usize,
    pub documents_remaining_before: usize,
    pub total_documents: usize,
}

impl Plan {
    pub fn compile(spec: PipelineSpec, registry: &OperatorRegistry) -> Result<Self> {
        let mut operators = Vec::new();

        for operator_node in &spec.pipeline {
            let operator: Box<dyn Operator> =
                registry.build(&operator_node.name, &operator_node.config)?;
            operators.push((operator_node.name.clone(), operator));
        }

        Ok(Self { operators, spec })
    }

    pub fn execute(&self) -> Result<ProcessingStatistics> {
        // Create output directory
        if let Some(parent) = Path::new(&self.spec.sink.uri).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create reader using factory
        let reader = ReaderFactory::create(&self.spec.source)?;
        let input_schema = reader.schema().clone();

        // Setup step-by-step output (lazy initialization - create writers only when needed)
        let mut step_writers: HashMap<usize, Box<dyn Writer>> = HashMap::new();
        let mut final_writer: Option<Box<dyn Writer>> = None;
        let mut err_writer: Option<Box<dyn Writer>> = None;

        // Pre-compute paths and file names for lazy writer creation
        let trace_base = format!("{}/trace", self.spec.sink.uri.trim_end_matches('/'));
        let final_base = format!("{}/final", self.spec.sink.uri.trim_end_matches('/'));
        let error_base = format!("{}/error", self.spec.sink.uri.trim_end_matches('/'));

        // Determine file name from input URI
        let input_file_name = Path::new(&self.spec.source.uris[0])
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file.jsonl");

        // Determine extension from sink kind or input file
        let extension = if self.spec.sink.kind == "parquet" || input_file_name.ends_with(".parquet")
        {
            ".parquet"
        } else {
            ".jsonl"
        };

        let file_name = input_file_name
            .replace(".parquet", extension)
            .replace(".jsonl", extension);

        // Step-by-step mode: track filtering at each step
        let mut total_rows = 0;
        let mut total_input_documents = 0;
        let mut step_stats: Vec<StepStatistics> = Vec::new();
        let mut documents_before_step: Vec<usize> = vec![0; self.operators.len()];
        let mut documents_removed_at_step: Vec<usize> = vec![0; self.operators.len()];
        let mut step_processing_times: Vec<std::time::Duration> =
            vec![std::time::Duration::ZERO; self.operators.len()];

        // Create progress bar
        let progress = ProgressBar::new_spinner();
        progress.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} [{elapsed_precise}] Processed: {pos:>7} documents")
                .unwrap(),
        );
        progress.enable_steady_tick(std::time::Duration::from_millis(100));

        // Track I/O times
        let mut write_time = std::time::Duration::ZERO;

        // Process samples from reader (generator-like API)
        // Note: Read time is difficult to measure accurately in iterator-based API
        // as the actual disk I/O happens inside the iterator's next() method.
        // For Parquet, reading is batched, so individual sample reads are very fast.
        for sample_result in reader {
            match sample_result {
                Ok(sample) => {
                    let mut filtered_at_step: Option<usize> = None;
                    let mut final_sample: Option<Sample> = None;
                    let mut sample_before_step: Option<Sample> = None;
                    let mut sample_opt: Option<Sample> = Some(sample);
                    let enable_trace = self.spec.sink.enable_trace;

                    for (step_idx, (_name, op)) in self.operators.iter().enumerate() {
                        // Track documents that reached this step
                        documents_before_step[step_idx] += 1;

                        // Take sample from Option
                        let current_sample = match sample_opt.take() {
                            Some(s) => {
                                // Only clone if trace is enabled (for trace output when filtered)
                                if enable_trace {
                                    sample_before_step = Some(s.clone());
                                }
                                s
                            }
                            None => break, // Should not happen
                        };

                        // Measure processing time for this step
                        let step_start = std::time::Instant::now();
                        let result = op.process(current_sample);
                        let step_duration = step_start.elapsed();
                        step_processing_times[step_idx] += step_duration;

                        match result {
                            Ok(Some(modified)) => {
                                sample_opt = Some(modified); // Continue with modified sample
                            }
                            Ok(None) => {
                                // Sample was filtered out - write to trace output
                                filtered_at_step = Some(step_idx);
                                documents_removed_at_step[step_idx] += 1;
                                break; // Filtered out at this step
                            }
                            Err(_) => {
                                // Error during processing - write to trace output
                                filtered_at_step = Some(step_idx);
                                documents_removed_at_step[step_idx] += 1;
                                break; // Filter out on error
                            }
                        }
                    }

                    // Only clone if we passed all steps (for final output)
                    // This reduces cloning: we only clone once at the end for successful samples
                    if let Some(s) = sample_opt {
                        final_sample = Some(s.clone());
                    }

                    // Write to appropriate step directory
                    if let Some(step_idx) = filtered_at_step {
                        // Write to step_XX directory (the sample before it was filtered)
                        // Only if trace is enabled
                        if enable_trace {
                            // Create writer lazily if needed
                            if let std::collections::hash_map::Entry::Vacant(e) =
                                step_writers.entry(step_idx)
                            {
                                let step_dir = format!("{}/step_{:02}", trace_base, step_idx);
                                std::fs::create_dir_all(&step_dir)?;
                                // Use directory as URI to enable sharding if samples_per_shard > 0
                                // Otherwise use file path
                                let step_uri = if self.spec.sink.samples_per_shard > 0 {
                                    step_dir.clone()
                                } else {
                                    format!("{}/{}", step_dir, file_name)
                                };
                                let writer = WriterFactory::create(
                                    &crate::spec::SinkSpec {
                                        kind: self.spec.sink.kind.clone(),
                                        uri: step_uri,
                                        mode: "overwrite".to_string(),
                                        shard_key: None,
                                        samples_per_shard: self.spec.sink.samples_per_shard,
                                        shard_name_pattern: self
                                            .spec
                                            .sink
                                            .shard_name_pattern
                                            .clone(),
                                        enable_trace: false, // Trace writers don't need trace themselves
                                    },
                                    input_schema.clone(),
                                )?;
                                e.insert(writer);
                            }
                            if let Some(writer) = step_writers.get_mut(&step_idx) {
                                if let Some(sample_to_write) = sample_before_step {
                                    let write_start = std::time::Instant::now();
                                    writer.write_sample(sample_to_write)?;
                                    write_time += write_start.elapsed();
                                }
                            }
                        }
                    } else if let Some(final_sample_value) = final_sample {
                        // Write to step_final directory
                        // Create writer lazily if needed
                        if final_writer.is_none() {
                            std::fs::create_dir_all(&final_base)?;
                            // Use directory as URI to enable sharding if samples_per_shard > 0
                            // Otherwise use file path
                            let final_uri = if self.spec.sink.samples_per_shard > 0 {
                                final_base.clone()
                            } else {
                                format!("{}/{}", final_base, file_name)
                            };
                            final_writer = Some(WriterFactory::create(
                                &crate::spec::SinkSpec {
                                    kind: self.spec.sink.kind.clone(),
                                    uri: final_uri,
                                    mode: "overwrite".to_string(),
                                    shard_key: None,
                                    samples_per_shard: self.spec.sink.samples_per_shard,
                                    shard_name_pattern: self.spec.sink.shard_name_pattern.clone(),
                                    enable_trace: false, // Final writer doesn't need trace
                                },
                                input_schema.clone(),
                            )?);
                        }
                        if let Some(ref mut w) = final_writer {
                            let write_start = std::time::Instant::now();
                            w.write_sample(final_sample_value)?;
                            write_time += write_start.elapsed();
                            total_rows += 1;
                        }
                    }
                }
                Err(e) => {
                    // Write to error writer (create lazily if needed)
                    if err_writer.is_none() {
                        std::fs::create_dir_all(&error_base)?;
                        let err_file_path = format!("{}/{}", error_base, file_name);
                        err_writer = Some(WriterFactory::create(
                            &crate::spec::SinkSpec {
                                kind: self.spec.sink.kind.clone(),
                                uri: err_file_path,
                                mode: "overwrite".to_string(),
                                shard_key: None,
                                samples_per_shard: 0, // Error files don't use sharding
                                shard_name_pattern: None,
                                enable_trace: false, // Error writer doesn't need trace
                            },
                            input_schema.clone(),
                        )?);
                    }
                    if let Some(ref mut err_w) = err_writer {
                        let mut error_sample = Sample::new();
                        error_sample.set_str("error", format!("{e}"));
                        let write_start = std::time::Instant::now();
                        err_w.write_sample(error_sample)?;
                        write_time += write_start.elapsed();
                    }
                }
            }

            total_input_documents += 1;
            // Update progress every 100 documents
            if total_input_documents % 100 == 0 {
                progress.set_position(total_input_documents as u64);
            }
        }

        // Finish progress bar
        progress.finish_with_message(format!("Processed {} documents", total_input_documents));

        // Close all writers and remove empty files
        for (step_idx, writer) in step_writers {
            if !writer.close()? {
                // No data written, remove the empty file/directory
                let step_dir = format!(
                    "{}/trace/step_{:02}",
                    self.spec.sink.uri.trim_end_matches('/'),
                    step_idx
                );
                // If sharding was enabled, ShardedWriter handles cleanup
                // If single file, try to remove it
                if self.spec.sink.samples_per_shard == 0 {
                    let file_path = format!("{}/{}", step_dir, file_name);
                    let _ = std::fs::remove_file(&file_path);
                }
            }
        }
        if let Some(w) = final_writer {
            if !w.close()? {
                // No data written, remove empty files/directories
                // If sharding was enabled, ShardedWriter handles cleanup
                // If single file, try to remove it
                if self.spec.sink.samples_per_shard == 0 {
                    let final_dir = format!("{}/final", self.spec.sink.uri.trim_end_matches('/'));
                    let file_path = format!("{}/{}", final_dir, file_name);
                    let _ = std::fs::remove_file(&file_path);
                }
            }
        }
        if let Some(w) = err_writer {
            if !w.close()? {
                // No data written, remove the empty file
                let error_dir = format!("{}/error", self.spec.sink.uri.trim_end_matches('/'));
                let file_path = format!("{}/{}", error_dir, file_name);
                let _ = std::fs::remove_file(&file_path);
            }
        }

        // Build step statistics
        for (step_idx, (name, _)) in self.operators.iter().enumerate() {
            let processing_time_ms = step_processing_times[step_idx].as_millis() as u64;
            let documents_remaining_before = documents_before_step[step_idx];
            let documents_removed = documents_removed_at_step[step_idx];

            step_stats.push(StepStatistics {
                step_name: name.clone(),
                step_index: step_idx,
                processing_time_ms,
                documents_removed,
                documents_remaining_before,
                total_documents: total_input_documents,
            });
        }

        // Read time is difficult to measure accurately in iterator-based API
        // as the actual disk I/O happens inside the iterator's next() method.
        // For Parquet, reading is batched, so individual sample reads are very fast.
        // We'll estimate it in the runner based on total time.
        let estimated_read_time_ms = 0; // Set to 0, will be calculated in runner

        Ok(ProcessingStatistics {
            num_documents: total_rows,
            step_statistics: step_stats,
            read_time_ms: estimated_read_time_ms,
            write_time_ms: write_time.as_millis() as u64,
        })
    }
}
