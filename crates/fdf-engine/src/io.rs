use arrow::datatypes::Schema;
use std::path::Path;
use std::sync::Arc;

use crate::spec::{SinkSpec, SourceSpec};

/// Type alias for writer creation function
type WriterFactoryFn =
    Box<dyn Fn(&str, Arc<Schema>) -> anyhow::Result<Box<dyn Writer>> + Send + Sync>;

// Reader trait and implementations
pub mod reader;

pub use reader::{jsonl::JsonlReader, multi_file::MultiFileReader, parquet::ParquetReader, Reader};

// Writer trait and implementations
pub mod writer;

pub use writer::{jsonl::JsonlWriter, parquet::ParquetWriter, sharded::ShardedWriter, Writer};

/// Factory for creating readers based on source configuration
pub struct ReaderFactory;

impl ReaderFactory {
    /// Create a reader from source spec
    /// Supports:
    /// - Single file: specify file path in uris
    /// - Multiple files: specify multiple file paths in uris
    /// - Directory: specify directory path in uris (reads all matching files in the directory)
    /// - HuggingFace dataset via hf:// protocol: use hf://datasets/org/dataset/path/to/file.parquet in uris
    /// - HuggingFace dataset via kind: specify kind="huggingface" with dataset name in uris (legacy)
    pub fn create(spec: &SourceSpec) -> anyhow::Result<Box<dyn Reader>> {
        // Handle HuggingFace datasets
        if spec.kind == "huggingface" || spec.kind == "hf" {
            return Self::create_huggingface_reader(spec);
        }

        // Collect all file paths to read
        let mut file_paths = Vec::new();

        for uri in &spec.uris {
            // Check for hf:// protocol (HuggingFace dataset)
            if uri.starts_with("hf://") {
                let local_path = Self::download_hf_dataset(uri)?;
                file_paths.push(local_path);
            } else {
                let path = Path::new(uri);
                if path.is_dir() {
                    // Read all matching files in the directory
                    let files = Self::list_files_in_directory(uri, &spec.kind)?;
                    file_paths.extend(files);
                } else if path.exists() {
                    // Single file
                    file_paths.push(uri.clone());
                } else {
                    return Err(anyhow::anyhow!("File or directory does not exist: {}", uri));
                }
            }
        }

        if file_paths.is_empty() {
            return Err(anyhow::anyhow!("No files found to read"));
        }

        // Create readers for each file
        let mut readers = Vec::new();
        for file_path in &file_paths {
            let reader: Box<dyn Reader> =
                if spec.kind == "parquet" || file_path.ends_with(".parquet") {
                    // For Parquet, use native projection for better performance
                    Box::new(reader::parquet::ParquetReader::with_options(
                        file_path,
                        spec.batch_size,
                        if spec.columns.mapping.is_empty() {
                            None
                        } else {
                            Some(spec.columns.mapping.clone())
                        },
                    )?)
                } else if spec.kind == "jsonl"
                    || spec.kind == "json"
                    || file_path.ends_with(".jsonl")
                    || file_path.ends_with(".json")
                {
                    // For JSONL, use column filter wrapper
                    let jsonl_reader = Box::new(reader::jsonl::JsonlReader::new(file_path)?);
                    if spec.columns.mapping.is_empty() {
                        jsonl_reader
                    } else {
                        Box::new(reader::column_filter::ColumnFilterReader::new(
                            jsonl_reader,
                            spec.columns.mapping.clone(),
                        )?)
                    }
                } else {
                    // Default to parquet
                    Box::new(reader::parquet::ParquetReader::with_options(
                        file_path,
                        spec.batch_size,
                        if spec.columns.mapping.is_empty() {
                            None
                        } else {
                            Some(spec.columns.mapping.clone())
                        },
                    )?)
                };
            readers.push(reader);
        }

        // If only one reader, return it directly; otherwise wrap in MultiFileReader
        if readers.len() == 1 {
            Ok(readers.into_iter().next().unwrap())
        } else {
            Ok(Box::new(reader::multi_file::MultiFileReader::new(readers)?))
        }
    }

    /// Download HuggingFace dataset file
    /// URI format: hf://datasets/org/dataset/path/to/file.parquet
    /// Examples:
    ///   - hf://datasets/HuggingFaceFW/fineweb-edu/CC-MAIN-2024-10/train-00000-of-00014.parquet
    ///   - hf://datasets/squad/train.parquet
    fn download_hf_dataset(uri: &str) -> anyhow::Result<String> {
        // Parse hf://datasets/org/dataset/path/to/file.parquet
        if !uri.starts_with("hf://datasets/") {
            return Err(anyhow::anyhow!(
                "Invalid HuggingFace URI format. Expected: hf://datasets/org/dataset/path/to/file.parquet"
            ));
        }

        let path = uri.strip_prefix("hf://datasets/").unwrap();
        let parts: Vec<&str> = path.split('/').collect();

        if parts.len() < 3 {
            return Err(anyhow::anyhow!(
                "Invalid HuggingFace URI format. Expected: hf://datasets/org/dataset/path/to/file.parquet"
            ));
        }

        // Extract dataset_id (first two parts: org/dataset)
        let dataset_id = format!("{}/{}", parts[0], parts[1]);
        // Extract file path (everything after org/dataset/)
        let mut file_path = parts[2..].join("/");

        // Remove common Git LFS path prefixes if present
        if file_path.starts_with("blob/main/") {
            file_path = file_path.trim_start_matches("blob/main/").to_string();
        } else if file_path.starts_with("blob/master/") {
            file_path = file_path.trim_start_matches("blob/master/").to_string();
        }

        // Use tokio runtime for async operations
        let rt = tokio::runtime::Runtime::new()?;

        rt.block_on(async {
            // Try to get HuggingFace token from environment variable
            let token = std::env::var("HF_TOKEN")
                .or_else(|_| std::env::var("HUGGINGFACE_TOKEN"))
                .or_else(|_| std::env::var("HF_API_TOKEN"))
                .ok();

            use hf_hub::api::tokio::ApiBuilder;
            let mut builder = ApiBuilder::new().with_progress(true);

            // Set token if available
            if let Some(token_value) = token {
                builder = builder.with_token(Some(token_value));
            }

            let api = builder.build()?;
            let repo = api.dataset(dataset_id);

            // Download the file
            let local_file = repo.get(&file_path).await?;

            Ok(local_file.display().to_string())
        })
    }

    /// List all files in a directory that match the specified kind
    fn list_files_in_directory(dir: &str, kind: &str) -> anyhow::Result<Vec<String>> {
        let path = Path::new(dir);
        if !path.is_dir() {
            return Err(anyhow::anyhow!("Path is not a directory: {}", dir));
        }

        let mut files = Vec::new();
        let entries = std::fs::read_dir(dir)?;

        // Determine file extensions to match
        let extensions: Vec<&str> = match kind {
            "parquet" => vec![".parquet"],
            "jsonl" | "json" => vec![".jsonl", ".json"],
            _ => vec![".parquet", ".jsonl", ".json"], // Default: match all supported formats
        };

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(extension) = path.extension() {
                    let ext_str = extension.to_string_lossy().to_lowercase();
                    if extensions
                        .iter()
                        .any(|&ext| ext_str == ext.trim_start_matches('.'))
                    {
                        if let Some(path_str) = path.to_str() {
                            files.push(path_str.to_string());
                        }
                    }
                }
            }
        }

        // Sort files for consistent ordering
        files.sort();

        Ok(files)
    }

    /// Create a reader for HuggingFace datasets
    fn create_huggingface_reader(spec: &SourceSpec) -> anyhow::Result<Box<dyn Reader>> {
        if spec.uris.is_empty() {
            return Err(anyhow::anyhow!(
                "HuggingFace dataset requires at least one URI (dataset identifier)"
            ));
        }

        // For multiple URIs, create multiple readers and combine them
        let mut readers: Vec<Box<dyn Reader>> = Vec::new();
        for uri in &spec.uris {
            let reader: Box<dyn Reader> =
                Box::new(reader::huggingface::HuggingFaceReader::new(uri)?);
            readers.push(reader);
        }

        // Combine readers if multiple
        let combined_reader: Box<dyn Reader> = if readers.len() > 1 {
            Box::new(reader::multi_file::MultiFileReader::new(readers)?)
        } else {
            readers.into_iter().next().unwrap()
        };

        // Apply column filter if column mapping is specified
        if spec.columns.mapping.is_empty() {
            Ok(combined_reader)
        } else {
            Ok(Box::new(reader::column_filter::ColumnFilterReader::new(
                combined_reader,
                spec.columns.mapping.clone(),
            )?))
        }
    }
}

/// Factory for creating writers based on sink configuration
pub struct WriterFactory;

impl WriterFactory {
    /// Create a writer from sink spec
    /// Automatically enables sharding if uri is a directory, disables if uri is a file
    pub fn create(spec: &SinkSpec, schema: Arc<Schema>) -> anyhow::Result<Box<dyn Writer>> {
        // Determine base writer type
        let is_parquet = spec.kind == "parquet" || spec.uri.ends_with(".parquet");

        // Check if uri is a directory or a file
        let path = Path::new(&spec.uri);
        // If uri ends with a known extension, treat as file; otherwise treat as directory
        let is_directory = !spec.uri.ends_with(".parquet")
            && !spec.uri.ends_with(".jsonl")
            && !spec.uri.ends_with(".json")
            && (path.is_dir() || !path.exists() || spec.uri.ends_with('/'));

        // Enable sharding if uri is a directory
        if is_directory {
            // Create directory if it doesn't exist
            std::fs::create_dir_all(&spec.uri)?;

            let create_writer: WriterFactoryFn = if is_parquet {
                Box::new(|path: &str, s: Arc<Schema>| {
                    Ok(Box::new(ParquetWriter::new(path, s)?) as Box<dyn Writer>)
                })
            } else {
                Box::new(|path: &str, s: Arc<Schema>| {
                    Ok(Box::new(JsonlWriter::new(path, s)?) as Box<dyn Writer>)
                })
            };

            // Determine default shard name pattern based on extension
            let default_pattern = if is_parquet {
                "part-{shard_id:08}.parquet"
            } else {
                "part-{shard_id:08}.jsonl"
            };

            Ok(Box::new(writer::sharded::ShardedWriter::new(
                &spec.uri,
                schema,
                spec.shard_key.clone(),
                spec.samples_per_shard,
                spec.shard_name_pattern
                    .clone()
                    .or_else(|| Some(default_pattern.to_string())),
                create_writer,
            )?) as Box<dyn Writer>)
        } else {
            // Create regular (non-sharded) writer for file path
            let writer: Box<dyn Writer> = if is_parquet {
                Box::new(writer::parquet::ParquetWriter::new(&spec.uri, schema)?)
            } else if spec.kind == "jsonl"
                || spec.kind == "json"
                || spec.uri.ends_with(".jsonl")
                || spec.uri.ends_with(".json")
            {
                Box::new(writer::jsonl::JsonlWriter::new(&spec.uri, schema)?)
            } else {
                // Default to parquet
                Box::new(writer::parquet::ParquetWriter::new(&spec.uri, schema)?)
            };
            Ok(writer)
        }
    }
}
