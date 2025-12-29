use crate::io::{ReaderFactory, Writer, WriterFactory};
use crate::spec::PipelineSpec;
use fdf_sdk::{Operator, OperatorRegistry, Result, Sample};
use std::collections::HashMap;
use std::path::Path;

pub struct Plan {
    operators: Vec<(String, Box<dyn Operator>)>,
    spec: PipelineSpec,
}

pub struct ProcessingStatistics {
    pub num_documents: usize,
    pub step_statistics: Vec<StepStatistics>,
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

        // Process samples from reader (generator-like API)
        for sample_result in reader {
            total_input_documents += 1;
            match sample_result {
                Ok(mut sample) => {
                    // Track the sample before each operator
                    let mut sample_before_step: Option<Sample> = None;
                    let mut filtered_at_step: Option<usize> = None;
                    let mut final_sample: Option<Sample> = None;

                    for (step_idx, (_name, op)) in self.operators.iter().enumerate() {
                        // Save sample before processing this step (clone the current sample)
                        sample_before_step = Some(sample.clone());

                        // Track documents that reached this step
                        documents_before_step[step_idx] += 1;

                        // Measure processing time for this step
                        let step_start = std::time::Instant::now();
                        let result = op.process(sample);
                        let step_duration = step_start.elapsed();
                        step_processing_times[step_idx] += step_duration;

                        match result {
                            Ok(Some(modified)) => {
                                sample = modified; // Continue with modified sample
                                                   // Save final sample at each step (will be overwritten until last step)
                                final_sample = Some(sample.clone());
                            }
                            Ok(None) => {
                                filtered_at_step = Some(step_idx);
                                documents_removed_at_step[step_idx] += 1;
                                break; // Filtered out at this step
                            }
                            Err(_) => {
                                filtered_at_step = Some(step_idx);
                                documents_removed_at_step[step_idx] += 1;
                                break; // Filter out on error
                            }
                        }
                    }

                    // Write to appropriate step directory
                    if let Some(step_idx) = filtered_at_step {
                        // Write to step_XX directory (the sample before it was filtered)
                        // Create writer lazily if needed
                        if let std::collections::hash_map::Entry::Vacant(e) =
                            step_writers.entry(step_idx)
                        {
                            let step_dir = format!("{}/step_{:02}", trace_base, step_idx);
                            std::fs::create_dir_all(&step_dir)?;
                            let step_file_path = format!("{}/{}", step_dir, file_name);
                            let writer = WriterFactory::create(
                                &crate::spec::SinkSpec {
                                    kind: self.spec.sink.kind.clone(),
                                    uri: step_file_path,
                                    mode: "overwrite".to_string(),
                                    shard_key: None,
                                    samples_per_shard: self.spec.sink.samples_per_shard,
                                    shard_name_pattern: None,
                                },
                                input_schema.clone(),
                            )?;
                            e.insert(writer);
                        }
                        if let Some(writer) = step_writers.get_mut(&step_idx) {
                            if let Some(sample_to_write) = sample_before_step {
                                writer.write_sample(sample_to_write)?;
                            }
                        }
                    } else if let Some(final_sample_value) = final_sample {
                        // Write to step_final directory
                        // Create writer lazily if needed
                        if final_writer.is_none() {
                            std::fs::create_dir_all(&final_base)?;
                            let final_file_path = format!("{}/{}", final_base, file_name);
                            final_writer = Some(WriterFactory::create(
                                &crate::spec::SinkSpec {
                                    kind: self.spec.sink.kind.clone(),
                                    uri: final_file_path,
                                    mode: "overwrite".to_string(),
                                    shard_key: None,
                                    samples_per_shard: self.spec.sink.samples_per_shard,
                                    shard_name_pattern: None,
                                },
                                input_schema.clone(),
                            )?);
                        }
                        if let Some(ref mut w) = final_writer {
                            w.write_sample(final_sample_value)?;
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
                                samples_per_shard: self.spec.sink.samples_per_shard,
                                shard_name_pattern: None,
                            },
                            input_schema.clone(),
                        )?);
                    }
                    if let Some(ref mut err_w) = err_writer {
                        let mut error_sample = Sample::new();
                        error_sample.set_str("error", format!("{e}"));
                        err_w.write_sample(error_sample)?;
                    }
                }
            }
        }

        // Close all writers and remove empty files
        for (step_idx, writer) in step_writers {
            if !writer.close()? {
                // No data written, remove the empty file
                let step_dir = format!(
                    "{}/trace/step_{:02}",
                    self.spec.sink.uri.trim_end_matches('/'),
                    step_idx
                );
                let file_path = format!("{}/{}", step_dir, file_name);
                let _ = std::fs::remove_file(&file_path);
            }
        }
        if let Some(w) = final_writer {
            if !w.close()? {
                // No data written, remove the empty file
                let final_dir = format!("{}/final", self.spec.sink.uri.trim_end_matches('/'));
                let file_path = format!("{}/{}", final_dir, file_name);
                let _ = std::fs::remove_file(&file_path);
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

        Ok(ProcessingStatistics {
            num_documents: total_rows,
            step_statistics: step_stats,
        })
    }
}
