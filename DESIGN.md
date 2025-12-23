# FoundationDataFactory — Design Document

## 0. What This Is

**FoundationDataFactory (FDF)** is a **streaming-first data execution system** for producing
training-ready datasets for **foundation models** (LLM / VLM / VLA).

FDF focuses on **data processing**, not training:

* ingest
* score
* evaluate
* filter
* refine
* generate
* mix
* materialize

FDF is designed for **scale, multimodality, GPU execution, and reproducibility**.

---

## 1. Design Goals

### Primary Goals

1. **Scale-first**

   * Handle very large datasets (100M–100B samples)
   * Avoid global shuffle by default
   * Predictable performance

2. **Streaming-first**

   * Native support for streaming datasets
   * Bounded memory usage
   * Batch-based processing

3. **Multimodal-native**

   * Text / image / video / tensor fields
   * Operators may require GPU acceleration

4. **Reproducible**

   * Deterministic execution given config + code version + seed
   * Stage-level materialization and manifests

5. **Composable**

   * Data recipes expressed as reusable operators
   * Operators can appear multiple times

6. **Observable**

   * Stage-level metrics and manifests
   * Sample-level provenance fields

### Non-Goals

* Training loop orchestration
* Model architectures
* Loss functions
* Hyperparameter tuning

FDF is **data infrastructure**, not a training framework.

---

## 2. Technology Stack (Hard Constraints)

FoundationDataFactory is built on a **Hugging Face Dataset + Daft + Ray** stack.

These are **intentional, non-accidental choices**.

### 2.1 Public Data Abstraction: Hugging Face Datasets (Required)

**Hugging Face `datasets.Dataset` / `IterableDataset` is the ONLY public data abstraction.**

Core rule:

> **Input to FDF is a Dataset. Output from FDF is a Dataset.**

FDF **must not** introduce a custom Dataset abstraction.

Benefits:

* ecosystem compatibility
* native streaming semantics
* minimal user-facing API surface
* seamless handoff to training pipelines

---

### 2.2 Data Execution Engine: Daft (Required)

Daft is the **internal data plane**:

* DataFrame semantics
* Columnar execution
* Complex / multimodal column support
* Rust-backed performance
* Parquet IO

Design constraint:

> All execution should map naturally to Daft operations
> (partitioned transforms, batch UDFs, columnar IO).

Daft is an **implementation detail** and must not leak into the public API.

---

### 2.3 Distributed Runtime: Ray (Required)

Ray is the **execution and scheduling plane**:

* Distributed partition execution
* CPU/GPU resource scheduling
* Long-lived GPU actor pools
* Backpressure and concurrency control

Design constraint:

> GPU-heavy operators MUST run inside Ray actors
> to amortize model load and maximize utilization.

---

### 2.4 Columnar Interchange: Apache Arrow (Implicit)

Arrow is the interchange layer between:

* Hugging Face Dataset batches
* Daft internal representation
* Operator inputs/outputs

Design constraint:

> Avoid Python object materialization in the hot path.
> Prefer Arrow / columnar batches.

---

### 2.5 Storage & Formats

Primary:

* Parquet (local filesystem or S3)

Optional (future):

* Lance (embedding-heavy datasets)

Design constraint:

* Stage outputs must be written as **shards + manifest**
* File sizes must be controlled to avoid small-file explosions

---

### 2.6 Optional Integrations (Plugins)

* **whylogs**: data profiling and drift metrics
* **Dagster**: orchestration / control-plane wrapper

Design constraint:

* These are optional hooks
* Core execution must not depend on them

---

## 3. Core Abstractions

### 3.1 Pipeline

A **pipeline** is a complete data recipe.

It defines:

* input dataset (or mixture)
* ordered stages
* output sink

Pipelines are **declarative and immutable**.

---

### 3.2 Stage

A **stage** is the fundamental execution unit.

A stage is the boundary for:

* fault tolerance
* caching / resume
* materialization
* profiling
* manifest emission

Stages:

* execute an operator graph over a dataset
* produce output shards
* emit a `manifest.json`

> **Stages are the only durable checkpoints in the system.**

---

### 3.3 Operator

Operators are atomic, reusable building blocks.

Important distinction:

* **Operator Type**: implementation class (e.g. `SigLIPEncoder`)
* **Operator Instance**: configured invocation with a unique `id`

Operators may appear **multiple times** in the same stage.

Each operator has a **kind**:

| kind        | Responsibility                    |
| ----------- | --------------------------------- |
| `score`     | Compute signals / scores / labels |
| `evaluator` | Audit correctness or quality      |
| `filter`    | Drop samples                      |
| `refiner`   | Modify or normalize fields        |
| `generator` | Generate new fields or samples    |

---

## 4. Dataset Abstraction (Critical)

### 4.1 Dataset In → Dataset Out

**This is a hard rule.**

```text
Dataset  →  FDF Pipeline  →  Dataset
```

Public API never exposes:

* Daft Dataset
* Ray Dataset
* Custom Dataset types

---

### 4.2 Dataset Lifecycle

1. **Ingest**

   * `datasets.load_dataset(...)`
   * local / S3 Parquet
   * streaming (`IterableDataset`) preferred

2. **Stage Execution**

   * dataset is sharded using HF semantics
   * shards processed independently
   * Daft used internally for execution

3. **Materialization**

   * stage outputs written as Parquet shards
   * `datasets.Dataset` reconstructed
   * `manifest.json` written

4. **Output**

   * final dataset returned
   * compatible with `.save_to_disk()` or training loaders

---

### 4.3 Streaming Semantics

If the input is an `IterableDataset`:

* streaming is preserved
* batch-based operators required
* global shuffle avoided by default

Streaming is the **default execution mode**.

---

## 5. Operator Contracts

### 5.1 General Rules

All operators must:

* declare input fields they read
* declare output fields they write
* preserve dataset schema consistency
* write provenance metadata

---

### 5.2 Kind-Specific Rules

#### score

* adds fields only
* must not drop rows
* recommended naming: `*_score`, `*_label`, `*_reason`

#### evaluator

* adds `eval.*` or `__fdf__/eval_*` fields
* must not change dataset semantics

#### filter

* may drop rows
* must set:

  ```text
  __fdf__/dropped_by = <operator_instance_id>
  ```

#### refiner

* modifies specific fields
* must append:

  ```text
  __fdf__/refined_by += [<id>]
  ```

#### generator

* generates new fields or rows
* must append:

  ```text
  __fdf__/generated_by += [<id>]
  ```

* may be non-deterministic
* should support seeding when possible

---

## 6. Execution Model

### 6.1 Planning Phase

1. Parse YAML into `PipelineConfig`
2. Validate:

   * unique stage names
   * unique operator ids per stage
   * kind compatibility
3. Normalize:

   * linear operators → implicit DAG
   * optional explicit `edges`
4. Build physical plan:

   * sharding strategy
   * batching strategy
   * CPU vs GPU placement
   * stage materialization config

---

### 6.2 Runtime Phase (Daft + Ray)

* HF Dataset shards define parallelism
* Daft executes columnar transforms
* Ray schedules shards and GPU actors
* GPU operators run in long-lived actor pools
* Stage completion commits manifest

---

## 7. Stage Materialization & Resume

### 7.1 Materialization

Each stage writes:

* Parquet shard files
* `manifest.json`

### 7.2 Resume Semantics

* `mode=incremental`:

  * already-committed shards skipped
* `mode=overwrite`:

  * delete and rerun stage

Resume always happens at **stage granularity**.

---

## 8. Data Mixture

### 8.1 Input Mixture

Input may be a mixture of datasets with:

* weights
* caps (rows / tokens / images / bytes)
* streaming interleave

Implementation:

* probabilistic interleaving
* local counters
* no global shuffle

---

### 8.2 Output Mixture

Mixture after scoring:

* bucket by predicates
* sample by quota
* approximate ratios preferred over shuffle

---

### 8.3 Curriculum

Mixture weights may vary with an external variable
(e.g., training step).

Resolved at runtime.

---

## 9. Observability

### 9.1 Manifests

Each stage emits:

```json
{
  "pipeline": "...",
  "stage": "...",
  "run_id": "...",
  "input_rows": 1200000,
  "output_rows": 830000,
  "operator_versions": {...},
  "outputs": [...],
  "profiles": [...],
  "config_hash": "...",
  "code_version": "git:..."
}
```

---

### 9.2 Sample-Level Provenance

Reserved fields:

* `__fdf__/tags`
* `__fdf__/scores`
* `__fdf__/dropped_by`
* `__fdf__/refined_by`
* `__fdf__/generated_by`
* `__fdf__/operator_versions`

---

## 10. Hooks & Plugins

Hooks:

* `on_stage_start`
* `on_partition_end`
* `on_stage_end`

Plugins:

* whylogs profiling
* Dagster orchestration wrapper

Hooks must not affect core execution semantics.

---

## 11. Repository Structure (Reference)

```
foundation-data-factory/
  README.md
  DESIGN.md
  src/fdf/
    cli/
    config/
    planner/
    runtime/
    io/
    mixture/
    operators/
    lineage/
    hooks/
  pipelines/
  tests/
```

---

## 12. Summary

FoundationDataFactory is:

* **HF Dataset in → HF Dataset out**
* **Daft for data execution**
* **Ray for distributed & GPU execution**
* **Stage-based, resumable, observable**
* **Designed for foundation-model scale**

This design intentionally constrains implementation freedom
to ensure long-term correctness, performance, and usability.

---
