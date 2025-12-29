use arrow::datatypes::Schema;
use std::path::Path;
use std::sync::Arc;

use crate::spec::{SinkSpec, SourceSpec};

/// Type alias for writer creation function
type WriterFactoryFn =
    Box<dyn Fn(&str, Arc<Schema>) -> anyhow::Result<Box<dyn Writer>> + Send + Sync>;

// Reader trait and implementations
pub mod reader;

pub use reader::{jsonl::JsonlReader, parquet::ParquetReader, Reader};

// Writer trait and implementations
pub mod writer;

pub use writer::{jsonl::JsonlWriter, parquet::ParquetWriter, sharded::ShardedWriter, Writer};

/// Factory for creating readers based on source configuration
pub struct ReaderFactory;

impl ReaderFactory {
    /// Create a reader from source spec
    pub fn create(spec: &SourceSpec) -> anyhow::Result<Box<dyn Reader>> {
        // Use first URI for now (can be extended to support multiple URIs)
        let path = &spec.uris[0];

        // Determine reader type based on file extension or source kind
        let reader: Box<dyn Reader> = if spec.kind == "parquet" || path.ends_with(".parquet") {
            Box::new(reader::parquet::ParquetReader::new(path)?)
        } else if spec.kind == "jsonl"
            || spec.kind == "json"
            || path.ends_with(".jsonl")
            || path.ends_with(".json")
        {
            Box::new(reader::jsonl::JsonlReader::new(path)?)
        } else {
            // Default to parquet
            Box::new(reader::parquet::ParquetReader::new(path)?)
        };

        Ok(reader)
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
