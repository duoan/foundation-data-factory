# FoundationDataFactory

> **The data factory for foundation model training.**
> A high-performance, Rust-based, multimodal data execution system.

FoundationDataFactory (FDF) turns research "data recipes" into **production-grade pipelines**:
ingest → annotate → filter → refine → generate → materialize.

It is designed for **foundation-model scale** and **multimodal data** (text / image / video / audio),
with first-class support for **streaming processing**, **batch optimization**, and **reproducibility**.

[![Release](https://img.shields.io/github/v/release/duoan/foundation-data-factory)](https://img.shields.io/github/v/release/duoan/foundation-data-factory)
[![Build status](https://img.shields.io/github/actions/workflow/status/duoan/foundation-data-factory/ci.yml?branch=main)](https://github.com/duoan/foundation-data-factory/actions/workflows/ci.yml?query=branch%3Amain)
[![License](https://img.shields.io/github/license/duoan/foundation-data-factory)](https://img.shields.io/github/license/duoan/foundation-data-factory)

---

- **Github repository**: <https://github.com/duoan/foundation-data-factory/>
- **Documentation** <https://duoan.github.io/foundation-data-factory/>

## Why FoundationDataFactory?

Data quality and composition increasingly dominate foundation model outcomes.
Papers propose filters, dedup methods, scorers, and synthetic captioning — but reproducing them is painful:

- ad-hoc scripts are not reproducible
- "one-off" pipelines don't scale
- Python-based pipelines are too slow for large-scale data processing
- multimodal types (image/video/tensors) don't fit traditional data stacks

**FDF fills the missing layer between papers and pretraining with a high-performance Rust implementation.**

---

## Core Concepts

### Pipeline

A pipeline is the full recipe from input(s) to output. Each pipeline consists of one or more stages that process data sequentially.

### Stage

A stage is the **fault-tolerance + caching + materialization boundary**.

- Each stage has its own `input` and `output` configuration
- Stages can be resumed and audited
- The first stage's input is the pipeline's overall input
- The last stage's output is the pipeline's overall output
- Intermediate stages automatically chain: previous stage's output becomes next stage's input

### Operator

Operators are atomic, reusable building blocks that operate on columnar data using Apache Arrow RecordBatches.
Operators can appear **multiple times** in a stage.

Each operator has a `kind` (inspired by modern data-recipe frameworks):

| kind        | What it does                                                                    |
|-------------|----------------------------------------------------------------------------------|
| `annotator` | Compute signals/scores/labels (adds fields prefixed with `__annotation_`, doesn't drop rows) |
| `evaluator` | Audit quality/correctness/consistency (adds eval fields)                         |
| `filter`    | Drop samples based on conditions                                                  |
| `refiner`   | Normalize/clean/transform fields (records provenance)                           |
| `generator` | Generate new fields or new samples (caption/OCR/synthesis)                       |

### Built-in Operators

- **textstat-annotator**: Calculate text readability and structure metrics (adds `__annotation_textstat_*` fields)
- **textstat-filter**: Filter text based on textstat metrics with configurable thresholds

See `src/operators/` for the full list of available operators.

---

## Installation

### Prerequisites

- Rust 1.70+ ([install](https://rustup.rs/))

### Build from Source

```bash
# Clone the repository
git clone https://github.com/duoan/foundation-data-factory.git
cd foundation-data-factory

# Build release binary
cargo build --release

# Binary will be at: target/release/fdf
```

---

## Quick Start (YAML Recipe)

Create `pipeline.yaml`:

```yaml
name: "example_pipeline"

stages:
  - name: "process_text"
    input:
      source:
        type: "parquet"
        path: "./input/data.parquet"
    operators:
      - "textstat-annotator":
          column: "text"
    output:
      source:
        type: "parquet"
        path: "./output/stage1"
```

Run:

```bash
./target/release/fdf run pipeline.yaml
```

### Example with Text Filtering

For a more complete example using text statistics filtering:

```yaml
name: "example_pipeline_with_filter"

stages:
  - name: "score_and_filter"
    input:
      source:
        type: "parquet"
        path: "./input/data.parquet"
    operators:
      - "textstat-annotator"
      - "textstat-filter":
          column: "text"
          metrics:
            flesch_reading_ease:
              min: 0
              max: 100
            sentence_count:
              min: 1.0
              max: 89.1
            character_count:
              min: 118.0
              max: 7466.3
    output:
      source:
        type: "parquet"
        path: "./output/filtered"
```

See `example_pipeline_with_filter.yaml` for a complete example.

---

## Architecture

FDF is built on a modern Rust data stack:

- **Apache Arrow**: Columnar in-memory format for efficient data processing
- **Apache Parquet**: Columnar storage format for persistent data
- **Direct Arrow/Parquet manipulation**: No high-level DataFrame overhead, maximum performance

### Key Features

- **Streaming-first**: Process data batch-by-batch, writing output incrementally
- **High performance**: Rust-native implementation with LLVM optimizations
- **Memory efficient**: Zero-cost abstractions, precise memory control
- **Partitioned output**: Automatically splits large outputs into multiple Parquet files
- **Progress tracking**: Real-time batch processing progress with progress bars

### Performance

The Rust implementation provides significant performance improvements over Python-based alternatives:

- **10-100x faster** processing (depending on data size and operations)
- **Lower memory footprint** with zero-cost abstractions
- **No Python overhead** - direct Arrow/Parquet manipulation

---

## Observability & Reproducibility

Every stage emits a `manifest.json` with:

- Stage name and output path
- Input/output row counts
- Operator-level statistics (input rows, output rows, filtered rows)
- Partition file paths
- Operator versions and parameters

Example manifest:

```json
{
  "pipeline_name": "example_pipeline_with_filter",
  "stages": [
    {
      "name": "score_and_filter",
      "input_path": "./input/data.parquet",
      "output_path": "./output/filtered",
      "partition_files": ["part-00000.parquet", "part-00001.parquet"],
      "operators": [
        {
          "name": "textstat-annotator",
          "kind": "annotator",
          "input_rows": 50000,
          "output_rows": 50000
        },
        {
          "name": "textstat-filter",
          "kind": "filter",
          "input_rows": 50000,
          "output_rows": 49868,
          "filtered_rows": 132
        }
      ],
      "total_input_rows": 50000,
      "total_output_rows": 49868
    }
  ]
}
```

---

## CLI Usage

FDF provides a command-line interface for running pipelines:

```bash
# Validate a pipeline configuration
./target/release/fdf validate pipeline.yaml

# Run a pipeline
./target/release/fdf run pipeline.yaml

# Show version
./target/release/fdf version
```

---

## Data Sources

FDF supports multiple data source formats for both input and output:

- **parquet**: Apache Parquet files (supports single file or directory)
- **huggingface**: Hugging Face datasets (via Parquet files)

Each stage can read from and write to any supported format. Data is automatically converted between formats as it flows through the pipeline.

---

## Development

### Setup

```bash
# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Clone and build
git clone https://github.com/duoan/foundation-data-factory.git
cd foundation-data-factory
cargo build --release
```

### Running Tests

```bash
cargo test
```

### Code Quality

```bash
# Format code
cargo fmt

# Lint
cargo clippy --all-targets --all-features -- -D warnings

# Check compilation
cargo check
```

### Pre-commit Hooks

Install pre-commit hooks:

```bash
pre-commit install
```

The hooks will automatically run `cargo fmt`, `cargo clippy`, and `cargo check` on commit.

---

## Contributing

Contributions welcome:

- new operators (with paper reference)
- new pipeline templates
- performance improvements
- tests and benchmarks
- support for additional data formats

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.

---

## Acknowledgments

Inspired by [datamap-rs](https://github.com/allenai/datamap-rs) for high-performance data processing patterns.
