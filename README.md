# Foundation Data Factory

High-performance data processing pipeline for LLM datasets using Rust and Apache Arrow.

## Quick Start

```bash
# Build the CLI
make build

# Run pipeline
make run

# Or directly
./target/release/fdf -c example_pipeline.yaml
```

## Features

- ✅ **Pure Rust** - Maximum performance with zero Python overhead
- ✅ **JSON-based Sample Architecture** - Simple, flexible data representation using `serde_json::Value`
- ✅ **Unified I/O** - Automatic reader/writer selection for Parquet and JSONL formats
- ✅ **Automatic Sharding** - Automatically shard output files when writing to directories
- ✅ **Step-by-step Trace Output** - Track documents filtered at each pipeline step
- ✅ **Comprehensive Statistics** - Detailed processing metrics and per-step statistics
- ✅ **YAML Configuration** - Declarative pipeline definitions
- ✅ **Easy to Extend** - Add new operators as Rust traits

## Architecture

The pipeline processes data using a row-by-row approach with `Sample` objects, which are wrappers around `serde_json::Value`. Each operator processes one sample at a time, making it easy to understand and extend.

### Data Flow

1. **Reader** reads data from source (Parquet or JSONL) and yields `Sample` objects
2. **Operators** process samples sequentially:
   - **Filters**: Return `Some(sample)` to keep, `None` to filter out
   - **Transformers**: Modify samples and return `Some(modified_sample)`
   - **Annotators**: Add fields to samples and return `Some(annotated_sample)`
3. **Writer** writes samples to sink (Parquet or JSONL), automatically sharding when writing to directories

### Output Structure

When processing, the pipeline automatically creates a structured output:

```text
output/
├── trace/
│   ├── step_00/          # Documents filtered at step 0
│   ├── step_01/          # Documents filtered at step 1
│   └── step_02/          # Documents filtered at step 2
├── final/                # Documents that passed all filters
└── error/                # Documents that failed to parse
```

Each output file maintains the same name as its input file. Empty files are automatically removed.

## Available Operators

### Common Operators

- `add_id` - Adds UUID4 identifier to each record
- `numeric_range_filter` - Filters by numeric field values with optional range negation

### Text Operators

**Transformers:**

- `text_normalize_transformer` - Text normalization (lowercase, strip whitespace)

**Filters:**

- `text_len_filter` - Filter by text length range
- `text_symbol_ratio_filter` - Filter by symbol-to-word ratio
- `text_gopher_quality_filter` - Gopher quality heuristics (TODO)
- `text_gopher_repetition_filter` - Gopher repetition detection (TODO)
- `text_fasttext_classifier_filter` - FastText classification (TODO)

## Example Configuration

```yaml
source:
  kind: parquet
  uris:
    - /path/to/input.parquet
  columns:
    id: id
    text: text

pipeline:
  - add_id:
      id_col: "uuid"
  
  - text_normalize_transformer:
      text_col: text
      lowercase: true
      strip: true

  - text_len_filter:
      text_col: text
      lower_bound: 20
      upper_bound: 2000

  - text_symbol_ratio_filter:
      text_col: text
      max_symbol_to_word_ratio: 0.30

sink:
  kind: jsonline
  uri: ./output/              # Directory: enables automatic sharding
  mode: overwrite
  samples_per_shard: 10000     # Optional: samples per shard
  shard_name_pattern: "part-{shard_id:08}.jsonl"  # Optional: custom shard naming
```

### Configuration Notes

- **Source/Sink `kind`**: Can be `"parquet"`, `"jsonl"`, or `"json"`. Also auto-detected from file extension.
- **Directory vs File**: If `sink.uri` is a directory, automatic sharding is enabled. If it's a file path, no sharding.
- **Trace Output**: Automatically enabled. Creates `{uri}/trace/step_XX/` and `{uri}/final/` directories.
- **Error Output**: Automatically enabled. Creates `{uri}/error/` directory for parsing failures.

## Building

```bash
# Build release binary
make build

# Build debug binary
make build-dev

# Run tests
make test

# Format code
make fmt

# Run clippy
make clippy
```

## Statistics Output

After processing, the pipeline prints comprehensive statistics:

```text
=== Processing Statistics ===
Total processing time: 1.25 seconds
Number of documents processed: 41348

--- Pipeline Step Statistics ---
Step 0 (add_id)
  Processing time: 45ms (3.61%)
  Documents removed: 0 (0.00% of remaining, 0.00% of total)
Step 1 (text_normalize_transformer)
  Processing time: 29ms (2.33%)
  Documents removed: 0 (0.00% of remaining, 0.00% of total)
Step 2 (text_len_filter)
  Processing time: 5ms (0.40%)
  Documents removed: 8647 (17.29% of remaining, 17.29% of total)
Step 3 (text_symbol_ratio_filter)
  Processing time: 798ms (63.99%)
  Documents removed: 5 (0.01% of remaining, 0.01% of total)
============================

✓ Pipeline completed successfully
============================
```

## Adding New Operators

Operators are organized by modality (`text`, `image`, `video`, `audio`) and type (`filter`, `annotator`, `transformer`). Create a new file in the appropriate directory:

```rust
// crates/fdf-operators/src/text/filter/my_filter.rs
use fdf_sdk::{Operator, Result, Sample};

pub struct MyFilter {
    text_col: String,
    threshold: f64,
}

impl Operator for MyFilter {
    fn process(&self, sample: Sample) -> Result<Option<Sample>> {
        let text = sample
            .get_str(&self.text_col)
            .ok_or_else(|| anyhow::anyhow!("Missing field: {}", self.text_col))?;
        
        // Your filtering logic here
        if /* condition */ {
            Ok(Some(sample))  // Keep
        } else {
            Ok(None)          // Filter out
        }
    }
}

pub fn register(registry: &mut fdf_sdk::OperatorRegistry) {
    registry.register("my_filter", |config: &serde_yaml::Value| {
        let text_col = config["text_col"].as_str().unwrap().to_string();
        let threshold = config["threshold"].as_f64().unwrap_or(0.5);
        
        Ok(Box::new(MyFilter { text_col, threshold }))
    });
}
```

Then register it in the appropriate module file (e.g., `crates/fdf-operators/src/text/filter/mod.rs`).

### Operator Types

- **Filter**: Returns `Some(sample)` to keep, `None` to filter out
- **Transformer**: Modifies sample fields, returns `Some(modified_sample)`
- **Annotator**: Adds new fields to sample, returns `Some(annotated_sample)`

## Performance

Rust version provides **10-100x** performance improvement over Python:

- No Python interpreter overhead
- Zero-copy data processing with Apache Arrow
- Efficient JSON handling with `serde_json`
- Parallel processing ready (using `rayon`)

## License

See LICENSE file for details.
