# Foundation Data Factory

High-performance data processing pipeline for LLM datasets using Rust and Polars.

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
- ✅ **Polars Native** - Uses Polars expressions for vectorized operations
- ✅ **YAML Configuration** - Declarative pipeline definitions
- ✅ **Easy to Extend** - Add new operators as Rust traits

## Available Operators

- `text.normalize` - Text normalization (lowercase, strip)
- `text.len_filter` - Filter by text length range
- `text.special_char_ratio` - Calculate special character ratio
- `text.gopher_quality_filter` - Gopher quality heuristics (TODO)
- `text.gopher_repetition_filter` - Gopher repetition detection (TODO)
- `text.fasttext_classifier_filter` - FastText classification (TODO)
- `filter.leq` - Numeric filter (≤)
- `annotate.const` - Add constant annotation

## Example

See `example_pipeline.yaml` for a complete example pipeline configuration.

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

## Performance

Rust version provides **10-100x** performance improvement over Python:
- No Python interpreter overhead
- Native Polars expressions (no `map_elements`)
- Zero-copy data processing
- Full vectorization

## Adding New Operators

Create a new file in `crates/ops_text/src/`:

```rust
// crates/ops_text/src/my_filter.rs
use polars::prelude::*;
use fdf_sdk::{Operator, OperatorRegistry, Context, Result};

pub struct MyFilterOp {
    text_col: String,
}

impl Operator for MyFilterOp {
    fn apply(&self, df: LazyFrame, _ctx: &Context) -> Result<LazyFrame> {
        Ok(df.filter(col(&self.text_col).str().len_chars().gt(100)))
    }
}

pub fn register(registry: &mut OperatorRegistry) {
    registry.register_fn("my.filter", |config: &serde_yaml::Value| {
        let text_col = config["text_col"].as_str().unwrap().to_string();
        Ok(Box::new(MyFilterOp { text_col }))
    });
}
```

Then register it in `crates/ops_text/src/lib.rs`.
