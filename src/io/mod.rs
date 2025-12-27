use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::SerializedFileReader;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::config::DataSourceConfig;

/// Read data from a source and return as RecordBatch iterator
pub fn read_data_source(
    source: &DataSourceConfig,
) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>> {
    match source.source_type.as_str() {
        "parquet" => read_parquet(source),
        "huggingface" => read_huggingface(source),
        _ => anyhow::bail!("Unsupported data source type: {}", source.source_type),
    }
}

fn read_parquet(
    source: &DataSourceConfig,
) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>> {
    let path = source
        .path
        .as_ref()
        .context("path is required for parquet")?;

    let file = File::open(path)?;
    let file_reader = Arc::new(SerializedFileReader::new(file)?);
    let mut arrow_reader = parquet::arrow::ParquetFileArrowReader::new(file_reader);

    // Apply limit if specified
    let limit = source.limit.unwrap_or(usize::MAX);
    let mut count = 0;

    let mut batches: Vec<Result<RecordBatch, anyhow::Error>> = Vec::new();

    // Use the ArrowReader trait method
    let mut reader_iter =
        parquet::arrow::ArrowReader::get_record_reader(&mut arrow_reader, 1024 * 1024)?;

    while count < limit {
        match reader_iter.next() {
            Some(Ok(batch)) => {
                let rows = batch.num_rows();
                if count + rows > limit {
                    // Truncate last batch if needed
                    let remaining = limit - count;
                    let truncated = batch.slice(0, remaining);
                    batches.push(Ok(truncated));
                    break;
                }
                count += rows;
                batches.push(Ok(batch));
            }
            Some(Err(e)) => batches.push(Err(anyhow::Error::from(e))),
            None => break,
        }
    }

    Ok(Box::new(batches.into_iter()))
}

fn read_huggingface(
    source: &DataSourceConfig,
) -> Result<Box<dyn Iterator<Item = Result<RecordBatch>> + Send>> {
    let path = source
        .path
        .as_ref()
        .context("path is required for huggingface")?;

    // For Hugging Face datasets, read parquet files directly
    if path.ends_with(".parquet") {
        read_parquet(source)
    } else {
        anyhow::bail!(
            "Hugging Face dataset name resolution not yet implemented. Use direct parquet path."
        );
    }
}

pub fn write_parquet(batches: Vec<RecordBatch>, path: &Path) -> Result<()> {
    if batches.is_empty() {
        anyhow::bail!("No batches to write");
    }

    // Create directory if it doesn't exist
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write parquet file
    let file = File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(props))?;

    for batch in batches {
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(())
}
