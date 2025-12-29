use super::Writer;
use arrow::datatypes::Schema;
use fdf_sdk::Sample;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Type alias for writer creation function
type WriterFactoryFn = Box<dyn Fn(&str, Arc<Schema>) -> anyhow::Result<Box<dyn Writer>> + Send + Sync>;

/// Sharded writer that automatically writes to multiple shards based on samples per shard
pub struct ShardedWriter {
    writers: Mutex<HashMap<String, Box<dyn Writer>>>,
    shard_key: Option<String>,
    base_path: String, // Directory path
    #[allow(dead_code)] // Not used when base_path is directory
    base_name: String,
    extension: String, // File extension
    schema: Arc<Schema>,
    samples_per_shard: usize,
    shard_name_pattern: String, // Pattern for shard file names
    create_writer: WriterFactoryFn,
    current_shard_id: std::sync::atomic::AtomicUsize,
    current_shard_count: Mutex<usize>,
    // Track sample count per shard when using shard_key
    shard_key_counts: Mutex<HashMap<String, usize>>,
}

impl ShardedWriter {
    /// Create a new sharded writer
    /// - base_path: Base path for shard files (e.g., "output/data")
    /// - shard_key: Optional field name to use for sharding. If None, uses sequential sharding
    /// - samples_per_shard: Number of samples per shard before creating a new shard
    /// - shard_name_pattern: Pattern for shard file names. Supports placeholders:
    ///   - {base}: Base name without extension
    ///   - {shard_id}: Shard ID (zero-padded to 8 digits by default)
    ///   - {shard_id:08}: Shard ID with custom padding (e.g., :04 for 4 digits)
    ///   - {ext}: File extension
    ///
    ///   Default: "{base}.shard_{shard_id:08}.{ext}"
    /// - create_writer: Function to create individual writers
    pub fn new(
        base_path: &str,
        schema: Arc<Schema>,
        shard_key: Option<String>,
        samples_per_shard: usize,
        shard_name_pattern: Option<String>,
        create_writer: WriterFactoryFn,
    ) -> anyhow::Result<Self> {
        // base_path is a directory, extract extension from pattern or default to jsonl
        let extension = if shard_name_pattern
            .as_ref()
            .is_some_and(|p| p.ends_with(".parquet"))
        {
            ".parquet".to_string()
        } else if shard_name_pattern
            .as_ref()
            .is_some_and(|p| p.ends_with(".jsonl"))
        {
            ".jsonl".to_string()
        } else if shard_name_pattern
            .as_ref()
            .is_some_and(|p| p.ends_with(".json"))
        {
            ".json".to_string()
        } else {
            // Default to jsonl
            ".jsonl".to_string()
        };

        // Default pattern: "part-{shard_id:08}.{ext}"
        let pattern =
            shard_name_pattern.unwrap_or_else(|| format!("part-{{shard_id:08}}{}", extension));

        Ok(Self {
            writers: Mutex::new(HashMap::new()),
            shard_key,
            base_path: base_path.to_string(),
            base_name: String::new(), // Not used when base_path is directory
            extension,
            schema,
            samples_per_shard,
            shard_name_pattern: pattern,
            create_writer,
            current_shard_id: std::sync::atomic::AtomicUsize::new(0),
            current_shard_count: Mutex::new(0),
            shard_key_counts: Mutex::new(HashMap::new()),
        })
    }

    /// Get shard path for a given shard ID using the name pattern
    fn get_shard_path(&self, shard_id: usize) -> String {
        let mut result = self.shard_name_pattern.clone();

        // Replace {ext} if present
        result = result.replace("{ext}", &self.extension);

        // Replace {shard_id} with formatting
        // Support patterns like {shard_id:08} or just {shard_id}
        if result.contains("{shard_id:") {
            // Extract format specifier (e.g., "08" from "{shard_id:08}")
            let re = regex::Regex::new(r"\{shard_id:(\d+)\}").unwrap();
            if let Some(caps) = re.captures(&result) {
                if let Ok(width) = caps[1].parse::<usize>() {
                    let formatted = format!("{:0width$}", shard_id, width = width);
                    result = re.replace(&result, &formatted).to_string();
                }
            }
        } else {
            // Default: no padding, just the number
            result = result.replace("{shard_id}", &shard_id.to_string());
        }

        // Join with base_path (directory)
        std::path::Path::new(&self.base_path)
            .join(&result)
            .to_string_lossy()
            .to_string()
    }

    /// Get or create writer for a shard
    fn get_writer(&self, shard_id: usize) -> anyhow::Result<()> {
        let shard_id_str = format!("{:08}", shard_id);
        let mut writers = self.writers.lock().unwrap();
        if !writers.contains_key(&shard_id_str) {
            let shard_path = self.get_shard_path(shard_id);
            let writer = (self.create_writer)(&shard_path, self.schema.clone())?;
            writers.insert(shard_id_str.clone(), writer);
        }
        Ok(())
    }

    /// Determine shard ID and check if we need to advance to next shard
    /// Returns the shard ID to write to
    fn determine_shard_id(&self, sample: &Sample) -> anyhow::Result<usize> {
        if let Some(ref key) = self.shard_key {
            // Use field value for sharding
            if let Some(value) = sample.get_str(key) {
                // Get or create shard ID for this key value
                let mut counts = self.shard_key_counts.lock().unwrap();
                let count = counts.entry(value.to_string()).or_insert(0);

                // Calculate which shard this key value should go to
                // We use a simple hash to distribute key values across shards initially
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                value.hash(&mut hasher);
                let hash = hasher.finish();
                let base_shard = (hash % 1000) as usize; // Start with a base shard based on hash

                // If current shard is full, advance to next shard for this key
                let shard_id = if *count >= self.samples_per_shard {
                    // Find next available shard ID
                    let next_id = base_shard + (*count / self.samples_per_shard);
                    *count = 0;
                    next_id
                } else {
                    base_shard + (*count / self.samples_per_shard)
                };

                *count += 1;
                self.get_writer(shard_id)?;
                Ok(shard_id)
            } else {
                // Fallback to sequential sharding
                self.check_and_advance_shard()
            }
        } else {
            // Sequential sharding based on samples_per_shard
            self.check_and_advance_shard()
        }
    }

    /// Check if we need to move to next shard (for sequential sharding)
    fn check_and_advance_shard(&self) -> anyhow::Result<usize> {
        let mut count = self.current_shard_count.lock().unwrap();
        let current_id = self
            .current_shard_id
            .load(std::sync::atomic::Ordering::Relaxed);

        // If current shard is full, advance to next shard
        if *count >= self.samples_per_shard {
            let next_id = current_id + 1;
            self.current_shard_id
                .store(next_id, std::sync::atomic::Ordering::Relaxed);
            *count = 0;
            // Ensure writer exists for new shard
            self.get_writer(next_id)?;
            Ok(next_id)
        } else {
            *count += 1;
            Ok(current_id)
        }
    }
}

impl Writer for ShardedWriter {
    fn write_sample(&mut self, sample: Sample) -> anyhow::Result<()> {
        // Determine which shard to write to
        let shard_id = self.determine_shard_id(&sample)?;
        let shard_id_str = format!("{:08}", shard_id);

        // Ensure writer exists
        self.get_writer(shard_id)?;

        // Write to the shard writer
        let mut writers = self.writers.lock().unwrap();
        if let Some(writer) = writers.get_mut(&shard_id_str) {
            writer.write_sample(sample)?;
        }
        Ok(())
    }

    fn close(self: Box<Self>) -> anyhow::Result<bool> {
        // Close all shard writers
        let mut writers = self.writers.lock().unwrap();
        let mut has_any_data = false;
        for (_, writer) in writers.drain() {
            if writer.close()? {
                has_any_data = true;
            }
        }
        Ok(has_any_data)
    }

    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }
}
