# FoundationDataFactory

> **The data factory for foundation model training.**
> A streaming-first, scalable, multimodal data execution system.

FoundationDataFactory (FDF) turns research “data recipes” into **production-grade pipelines**:
ingest → score → evaluate → filter → refine → generate → mix → materialize.

It is designed for **foundation-model scale** and **multimodal data** (text / image / video / audio),
with first-class support for **distributed execution**, **GPU operators**, and **reproducibility**.

[![Release](https://img.shields.io/github/v/release/duoan/foundation-data-factory)](https://img.shields.io/github/v/release/duoan/foundation-data-factory)
[![Build status](https://img.shields.io/github/actions/workflow/status/duoan/foundation-data-factory/main.yml?branch=main)](https://github.com/duoan/foundation-data-factory/actions/workflows/main.yml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/duoan/foundation-data-factory/branch/main/graph/badge.svg)](https://codecov.io/gh/duoan/foundation-data-factory)
[![Commit activity](https://img.shields.io/github/commit-activity/m/duoan/foundation-data-factory)](https://img.shields.io/github/commit-activity/m/duoan/foundation-data-factory)
[![License](https://img.shields.io/github/license/duoan/foundation-data-factory)](https://img.shields.io/github/license/duoan/foundation-data-factory)

---

- **Github repository**: <https://github.com/duoan/foundation-data-factory/>
- **Documentation** <https://duoan.github.io/foundation-data-factory/>

## Why FoundationDataFactory?

Data quality and composition increasingly dominate foundation model outcomes.
Papers propose filters, dedup methods, scorers, and synthetic captioning — but reproducing them is painful:

- ad-hoc scripts are not reproducible
- “one-off” pipelines don’t scale
- GPUs are hard to schedule cleanly in ETL
- multimodal types (image/video/tensors) don’t fit traditional data stacks

**FDF fills the missing layer between papers and pretraining.**

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

Operators are atomic, reusable building blocks that operate on columnar data using Daft UDFs for distributed execution.
Operators can appear **multiple times** in a stage (each instance has a unique `id`).

Each operator has a `kind` (inspired by modern data-recipe frameworks):

| kind        | What it does                                                                    |
|-------------|----------------------------------------------------------------------------------|
| `score`     | Compute signals/scores/labels (adds fields, doesn't drop rows)                 |
| `evaluator` | Audit quality/correctness/consistency (adds eval fields)                         |
| `filter`    | Drop samples (records drop reason in metadata)                                  |
| `refiner`   | Normalize/clean/transform fields (records provenance)                           |
| `generator` | Generate new fields or new samples (caption/OCR/synthesis)                       |

### Built-in Operators

- **passthrough-refiner**: No-op operator for testing and pipeline development
- **textstat-filter**: Filter text based on readability and structure metrics (requires `textstat`)

See `fdf/operators/` for the full list of available operators.

---

## Installation

```bash
pip install foundation-data-factory
```

For optional text processing operators:

```bash
pip install foundation-data-factory[textstat]  # For textstat-filter operator
pip install foundation-data-factory[nltk]     # For text processing operators
```

## Quick Start (YAML Recipe)

Create `pipeline.yaml`:

```yaml
name: "example_pipeline"

stages:
  - name: "process_text"
    input:
      source:
        type: "huggingface"
        path: "imdb"
        streaming: false
    operators:
      - id: "passthrough"
        kind: "refiner"
        op: "passthrough-refiner"

    output:
      source:
        type: "parquet"
        path: "./output/stage1"
```

Run:

```bash
fdf run pipeline.yaml
```

### Example with Text Filtering

For a more complete example using text statistics filtering:

```yaml
name: "example_pipeline_with_filter"

stages:
  - name: "score_and_filter"
    input:
      source:
        type: "huggingface"
        path: "imdb"
        streaming: false
    operators:
      - id: "textstat_filter"
        kind: "filter"
        op: "textstat-filter"
        params:
          column: "text"
          metrics:
            - flesch_reading_ease
            - sentence_count
            - character_count
          min_scores:
            flesch_reading_ease: 0
            sentence_count: 1.0
            character_count: 100.0
          max_scores:
            flesch_reading_ease: 100
            sentence_count: 50.0
            character_count: 5000.0
    output:
      source:
        type: "parquet"
        path: "./output/filtered"
```

See `example_pipeline.yaml` and `example_pipeline_with_filter.yaml` for complete examples.

## Architecture

FDF is built on a modern data stack:

- **Hugging Face Datasets**: Primary data abstraction (input/output must be `datasets.Dataset` or `IterableDataset`)
- **Daft**: Internal data plane for columnar execution, DataFrame semantics, and distributed processing
- **Ray**: Distributed runtime for execution and scheduling, especially for GPU operators
- **Apache Arrow**: Columnar interchange format between Hugging Face Datasets, Daft, and operators

### Key Features

- **Streaming-first**: Native support for streaming datasets, avoiding global shuffle by default
- **Distributed execution**: Uses Daft's distributed UDFs for scalable processing
- **Unified runtime**: Single runtime handles both local and Ray execution via `daft.set_runner_ray()`
- **Data sharding**: Automatic sharding using Hugging Face's `datasets.distributed.split_dataset_by_node`
- **Multiple data formats**: Supports parquet, json, csv, iceberg, lance, and huggingface formats

## Observability & Reproducibility

Every stage emits a `manifest.json` with:

- stage name and output path
- output row count
- operator versions and parameters

Operators can add metadata to each row:

- **metadata**: JSON field containing operator-specific metrics and provenance
  - Example: `{"textstat-filter": {"flesch_reading_ease": 73.9, "sentence_count": 4.0}}`

## Optional Integrations

- **textstat**: Text readability and structure metrics for filtering (install with `pip install textstat`)
  - Used by `textstat-filter` operator for calculating metrics like Flesch reading ease, sentence count, etc.
- **NLTK**: Text processing operators like stopword removal (install with `pip install nltk`)

Integrations are optional and can be added as needed. Operators that require these dependencies will raise helpful error messages if the dependencies are missing.

To install with optional dependencies:

```bash
pip install foundation-data-factory[textstat,nltk]
```

## CLI Usage

FDF provides a command-line interface for running pipelines:

```bash
# Validate a pipeline configuration
fdf validate pipeline.yaml

# Run a pipeline locally
fdf run pipeline.yaml

# Run a pipeline on Ray cluster
fdf run pipeline.yaml --ray-address ray://127.0.0.1:10001
```

See `fdf/cli/main.py` for all available commands.

## Data Sources

FDF supports multiple data source formats for both input and output:

- **parquet**: Apache Parquet files (supports directory with manifest.json)
- **json**: JSON Lines format
- **csv**: Comma-separated values
- **huggingface**: Hugging Face datasets (local or remote)
- **iceberg**: Apache Iceberg tables (planned)
- **lance**: Lance format for vector data

Each stage can read from and write to any supported format. Data is automatically converted between formats as it flows through the pipeline.

## Contributing

Contributions welcome:

- new operators (with paper reference)
- new pipeline templates
- performance improvements
- tests and benchmarks
- support for additional data formats

## Getting started with your project

### 1. Create a New Repository

First, create a repository on GitHub with the same name as this project, and then run the following commands:

```bash
git init -b main
git add .
git commit -m "init commit"
git remote add origin git@github.com:duoan/foundation-data-factory.git
git push -u origin main
```

### 2. Set Up Your Development Environment

Then, install the environment and the pre-commit hooks with

```bash
make install
```

This will also generate your `uv.lock` file

### 3. Run the pre-commit hooks

Initially, the CI/CD pipeline might be failing due to formatting issues. To resolve those run:

```bash
uv run pre-commit run -a
```

### 4. Commit the changes

Lastly, commit the changes made by the two steps above to your repository.

```bash
git add .
git commit -m 'Fix formatting issues'
git push origin main
```

You are now ready to start development on your project!
The CI/CD pipeline will be triggered when you open a pull request, merge to main, or when you create a new release.

To finalize the set-up for publishing to PyPI, see the [publishing guide](https://fpgmaas.github.io/cookiecutter-uv/features/publishing/#set-up-for-pypi).
For activating the automatic documentation with MkDocs, see the [MkDocs guide](https://fpgmaas.github.io/cookiecutter-uv/features/mkdocs/#enabling-the-documentation-on-github).
To enable the code coverage reports, see the [codecov guide](https://fpgmaas.github.io/cookiecutter-uv/features/codecov/).

## Releasing a new version

- Create an API Token on [PyPI](https://pypi.org/).
- Add the API Token to your projects secrets with the name `PYPI_TOKEN` by visiting [this page](https://github.com/duoan/foundation-data-factory/settings/secrets/actions/new).
- Create a [new release](https://github.com/duoan/foundation-data-factory/releases/new) on Github.
- Create a new tag in the form `*.*.*`.

For more details, see the [CI/CD guide](https://fpgmaas.github.io/cookiecutter-uv/features/cicd/#how-to-trigger-a-release).

---

Repository initiated with [fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
