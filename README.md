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

A pipeline is the full recipe from input(s) to output.

### Stage

A stage is the **fault-tolerance + caching + materialization boundary**.
Stages can be resumed and audited.

### Operator

Operators are atomic, reusable building blocks.
Operators can appear **multiple times** in a stage (each instance has a unique `id`).

Each operator has a `kind` (inspired by modern data-recipe frameworks):

| kind | What it does |
|------|--------------|
| `score` | Compute signals/scores/labels (adds fields, doesn’t drop rows) |
| `evaluator` | Audit quality/correctness/consistency (adds eval fields) |
| `filter` | Drop samples (records drop reason) |
| `refiner` | Normalize/clean/transform fields (records provenance) |
| `generator` | Generate new fields or new samples (caption/OCR/synthesis) |

---

## Quick Start (YAML Recipe)

Create `pipeline.yaml`:

```yaml
name: "vlm-recipe-v1"

input:
  type: "mixture"
  seed: 42
  unit: "tokens"   # tokens | rows | images | bytes
  datasets:
    - name: "refinedweb"
      source: { type: "huggingface", path: "tiiuae/falcon-refinedweb" }
      weight: 0.6
    - name: "laion2b"
      source: { type: "huggingface", path: "laion/laion2B-en", streaming: true }
      weight: 0.4

stages:
  - name: "clean_and_score"
    materialize:
      path: "s3://fdf-runs/{{run_id}}/clean_and_score/"
      mode: "incremental"
    operators:
      - id: "score.text_quality"
        kind: "score"
        op: "TextQualityScorer"

      - id: "filter.text_quality"
        kind: "filter"
        op: "ThresholdFilter"
        params: { field: "text_quality_score", ge: 0.6 }

  - name: "embed_and_caption"
    materialize:
      path: "s3://fdf-runs/{{run_id}}/embed_and_caption/"
      mode: "incremental"
    operators:
      - id: "score.siglip_emb"
        kind: "score"
        op: "SigLIPEncoder"
        resources: { gpus: 0.5 }
        batch_size: 256
        write_fields: ["siglip_emb"]

      - id: "gen.caption"
        kind: "generator"
        op: "LLaVACaptioner"
        resources: { gpus: 1 }
        batch_size: 16

output:
  type: "parquet"
  path: "s3://my-data-lake/golden-dataset/"
```

Run:

```bash
fdf run pipeline.yaml
```

## Data Mixture (First-Class)

FDF supports mixtures that appear in real data recipes:

- dataset-level mixture: multiple sources with weights + caps
- quality-based mixture: bucketed sampling based on scores
- modality mixture: text-only / image-text / video-text ratios
- curriculum schedule: weights change over training steps

Mixture is designed to be streaming-first and avoids expensive global shuffle by default.

## Observability & Reproducibility

Every stage emits a manifest.json with:

- operator versions + parameters
- input/output counts and accept/reject stats
- output shard paths
- run metadata (seed, code version, config hash)

FDF also writes standard provenance fields into the dataset:

- **fdf**/tags
- **fdf**/scores
- **fdf**/dropped_by
- **fdf**/refined_by
- **fdf**/generated_by
- **fdf**/operator_versions

## Optional Integrations

- **Dagster**: orchestration and asset-based lineage (wrapper package)
- **LangKit**: Text metrics and filtering operators (install with `pip install langkit[all]`)
- **NLTK**: Text processing operators like stopword removal (install with `pip install nltk`)

Integrations are optional and can be added as needed. Operators that require these dependencies will raise helpful error messages if the dependencies are missing.

## Predefined Pipelines

FDF ships with curated pipeline templates inspired by papers:

- text/refinedweb_like.yaml
- vlm/laion_dedup_caption.yaml
- vlm/webdataset_exact_dedup.yaml
- eval/ocr_coverage_audit.yaml

```bash
fdf pipelines list
fdf pipelines show vlm/laion_dedup_caption
```

## Contributing

Contributions welcome:

- new operators (with paper reference)
- new pipeline templates
- performance improvements
- tests and benchmarks

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

To finalize the set-up for publishing to PyPI, see [here](https://fpgmaas.github.io/cookiecutter-uv/features/publishing/#set-up-for-pypi).
For activating the automatic documentation with MkDocs, see [here](https://fpgmaas.github.io/cookiecutter-uv/features/mkdocs/#enabling-the-documentation-on-github).
To enable the code coverage reports, see [here](https://fpgmaas.github.io/cookiecutter-uv/features/codecov/).

## Releasing a new version

- Create an API Token on [PyPI](https://pypi.org/).
- Add the API Token to your projects secrets with the name `PYPI_TOKEN` by visiting [this page](https://github.com/duoan/foundation-data-factory/settings/secrets/actions/new).
- Create a [new release](https://github.com/duoan/foundation-data-factory/releases/new) on Github.
- Create a new tag in the form `*.*.*`.

For more details, see [here](https://fpgmaas.github.io/cookiecutter-uv/features/cicd/#how-to-trigger-a-release).

---

Repository initiated with [fpgmaas/cookiecutter-uv](https://github.com/fpgmaas/cookiecutter-uv).
