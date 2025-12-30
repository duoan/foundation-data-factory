use super::Writer;
use arrow::datatypes::Schema;
use fdf_sdk::Sample;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

pub struct JsonlWriter {
    writer: BufWriter<File>,
    schema: Arc<Schema>,
    buffer: Vec<Sample>,
    partition_size: usize,
    path: String,           // Store path for potential deletion
    samples_written: usize, // Track number of samples written
}

impl JsonlWriter {
    pub fn new(path: &str, schema: Arc<Schema>) -> anyhow::Result<Self> {
        let output_file = File::create(path)?;
        let writer = BufWriter::new(output_file);
        Ok(Self {
            writer,
            schema,
            buffer: Vec::new(),
            partition_size: 50000, // Increased buffer size for better performance
            path: path.to_string(),
            samples_written: 0,
        })
    }

    /// Flush buffer to disk
    fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        
        // Serialize all samples in the buffer to a single string for better performance
        // This reduces the number of write syscalls
        let mut output = String::with_capacity(self.buffer.len() * 200); // Estimate 200 bytes per sample
        for sample in &self.buffer {
            let json_value = sample.as_value();
            let json_str = serde_json::to_string(json_value)?;
            output.push_str(&json_str);
            output.push('\n');
        }
        
        // Write all at once
        self.writer.write_all(output.as_bytes())?;
        self.samples_written += self.buffer.len();
        self.buffer.clear();
        
        // Don't flush BufWriter here - let it buffer automatically
        // Only flush when closing or when buffer is very large
        Ok(())
    }
}

impl Writer for JsonlWriter {
    fn write_sample(&mut self, sample: Sample) -> anyhow::Result<()> {
        self.buffer.push(sample);

        // Auto-flush when buffer reaches partition size
        if self.buffer.len() >= self.partition_size {
            self.flush()?;
        }

        Ok(())
    }

    fn close(mut self: Box<Self>) -> anyhow::Result<bool> {
        // Flush remaining samples
        self.flush()?;
        // Now flush the BufWriter to ensure all data is written to disk
        self.writer.flush()?;
        let has_data = self.samples_written > 0;

        // If no data was written, delete the file
        if !has_data {
            drop(self.writer); // Ensure file is closed before deletion
            let _ = std::fs::remove_file(&self.path);
        }

        Ok(has_data)
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
