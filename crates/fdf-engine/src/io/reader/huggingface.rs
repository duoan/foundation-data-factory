use super::Reader;
use arrow::datatypes::Schema;
use fdf_sdk::Sample;
use rayon::iter::*;
use std::sync::Arc;
/// HuggingFace dataset reader
/// Downloads dataset files from HuggingFace Hub and reads them
pub struct HuggingFaceReader {
    reader: Box<dyn Reader>,
}

impl HuggingFaceReader {
    /// Create a new HuggingFaceReader from a dataset identifier
    ///
    /// URI format:
    ///   - "dataset_name" - downloads default config and split
    ///   - "dataset_name/config" - downloads specific config
    ///   - "dataset_name/config/split" - downloads specific config and split
    ///   - "dataset_name/path/to/file.parquet" - downloads specific parquet file
    ///
    /// Examples:
    ///   - "squad" - downloads default config and split
    ///   - "squad/plain_text" - downloads specific config
    ///   - "squad/plain_text/train" - downloads specific config and split
    ///   - "HuggingFaceFW/fineweb-edu/blob/main/sample/10BT/000_00000.parquet" - specific file
    pub fn new(dataset_uri: &str) -> anyhow::Result<Self> {
        // Check if URI ends with .parquet (direct file path)
        if dataset_uri.ends_with(".parquet") {
            // Direct file path: org/dataset/path/to/file.parquet
            let parts: Vec<&str> = dataset_uri.split('/').collect();
            if parts.len() < 3 {
                return Err(anyhow::anyhow!(
                    "Invalid HuggingFace file URI format. Expected: org/dataset/path/to/file.parquet"
                ));
            }

            // Extract dataset_id (first two parts: org/dataset)
            let dataset_id = format!("{}/{}", parts[0], parts[1]);
            // Extract file path (everything after org/dataset/)
            let mut file_path = parts[2..].join("/");

            // Remove common Git LFS path prefixes if present
            // HuggingFace Hub file paths don't include "blob/main/" or "blob/master/"
            if file_path.starts_with("blob/main/") {
                file_path = file_path.trim_start_matches("blob/main/").to_string();
            } else if file_path.starts_with("blob/master/") {
                file_path = file_path.trim_start_matches("blob/master/").to_string();
            }

            // Use tokio runtime for async operations
            let rt = tokio::runtime::Runtime::new()?;

            let local_file = rt.block_on(async {
                Self::download_dataset_file_by_path(&dataset_id, &file_path).await
            })?;

            // Create a parquet reader for the downloaded file
            let reader: Box<dyn Reader> =
                Box::new(super::parquet::ParquetReader::new(&local_file)?);

            return Ok(Self { reader });
        }

        // Parse dataset identifier (format: dataset_name/config/split)
        // HuggingFace dataset names can have format: org/dataset_name/config/split
        let parts: Vec<&str> = dataset_uri.split('/').collect();

        let (dataset_id, config_name, split_name) = match parts.len() {
            1 => (parts[0].to_string(), None, None), // Just dataset name
            2 => {
                // Could be org/dataset or dataset/config - assume org/dataset for now
                (dataset_uri.to_string(), None, None)
            }
            3 => {
                // Could be org/dataset/config or dataset/config/split
                // Common split names: train, test, val, validation, dev
                let last_part = parts[2].to_lowercase();
                if matches!(
                    last_part.as_str(),
                    "train" | "test" | "val" | "validation" | "dev"
                ) {
                    // Format: dataset/config/split
                    (
                        format!("{}/{}", parts[0], parts[1]),
                        Some(parts[1]),
                        Some(parts[2]),
                    )
                } else {
                    // Format: org/dataset/config (treat as dataset name with config)
                    (format!("{}/{}", parts[0], parts[1]), Some(parts[2]), None)
                }
            }
            _ => {
                // 4+ parts: org/dataset/config/split
                let dataset_with_org = format!("{}/{}", parts[0], parts[1]);
                (dataset_with_org, Some(parts[2]), Some(parts[3]))
            }
        };

        // Use tokio runtime for async operations
        let rt = tokio::runtime::Runtime::new()?;

        let local_file = rt.block_on(async {
            Self::download_dataset_file(&dataset_id, config_name, split_name).await
        })?;

        // Create a parquet reader for the downloaded file
        // HuggingFace datasets are typically stored as parquet
        let reader: Box<dyn Reader> = Box::new(super::parquet::ParquetReader::new(&local_file)?);

        Ok(Self { reader })
    }

    /// Download a specific file from HuggingFace Hub by file path
    async fn download_dataset_file_by_path(
        dataset_id: &str,
        file_path: &str,
    ) -> anyhow::Result<String> {
        use hf_hub::api::tokio::ApiBuilder;

        // Try to get HuggingFace token from environment variable
        let token = std::env::var("HF_TOKEN")
            .or_else(|_| std::env::var("HUGGINGFACE_TOKEN"))
            .or_else(|_| std::env::var("HF_API_TOKEN"))
            .ok();

        let mut builder = ApiBuilder::new().with_progress(true);

        // Set token if available
        if let Some(token_value) = token {
            builder = builder.with_token(Some(token_value));
        }

        let api = builder.build()?;
        let repo = api.dataset(dataset_id.to_string());

        // Download the specific file
        let local_file = repo.get(file_path).await?;

        Ok(local_file.display().to_string())
    }

    /// Download dataset file from HuggingFace Hub
    async fn download_dataset_file(
        dataset_id: &str,
        config_name: Option<&str>,
        split_name: Option<&str>,
    ) -> anyhow::Result<String> {
        use hf_hub::api::tokio::ApiBuilder;

        // Try to get HuggingFace token from environment variable
        let token = std::env::var("HF_TOKEN")
            .or_else(|_| std::env::var("HUGGINGFACE_TOKEN"))
            .or_else(|_| std::env::var("HF_API_TOKEN"))
            .ok();

        let mut builder = ApiBuilder::new().with_progress(true);

        // Set token if available
        if let Some(token_value) = token {
            builder = builder.with_token(Some(token_value));
        }

        let api = builder.build()?;
        println!("Dataset {}", dataset_id);
        let repo = api.dataset(dataset_id.to_string());

        // Try to find parquet files for the dataset
        // HuggingFace datasets are typically stored in parquet format
        // Format: {split}-{shard_idx:05d}-of-{num_shards:05d}.parquet
        // or: {split}.parquet for single file

        // First, try to list files in the repo
        let repo_info = repo.info().await?;

        // Look for parquet files matching the split
        let target_split = split_name.unwrap_or("train");

        // Find parquet files for this split
        let mut parquet_files: Vec<String> = repo_info
            .siblings
            .par_iter()
            .filter_map(|sibling| {
                if let Some(filename) = sibling.rfilename.strip_suffix(".parquet") {
                    // Check if it matches the split
                    if filename == target_split
                        || filename.starts_with(&format!("{}-", target_split))
                    {
                        Some(sibling.rfilename.clone())
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        if parquet_files.is_empty() {
            // Try without split name (might be a single file dataset)
            parquet_files = repo_info
                .siblings
                .par_iter()
                .filter_map(|sibling| {
                    if sibling.rfilename.ends_with(".parquet") {
                        Some(sibling.rfilename.clone())
                    } else {
                        None
                    }
                })
                .collect();
        }

        if parquet_files.is_empty() {
            return Err(anyhow::anyhow!(
                "No parquet files found in dataset: {} (config: {:?}, split: {:?})",
                dataset_id,
                config_name,
                split_name
            ));
        }

        // Sort to get consistent ordering (use first file for now)
        // TODO: Support reading multiple shards
        parquet_files.sort();
        let filename = &parquet_files[0];

        // Download the file
        let local_file = repo.get(filename).await?;

        // Convert PathBuf to String
        Ok(local_file.display().to_string())
    }
}

impl Iterator for HuggingFaceReader {
    type Item = anyhow::Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

impl Reader for HuggingFaceReader {
    fn schema(&self) -> &Arc<Schema> {
        self.reader.schema()
    }
}
