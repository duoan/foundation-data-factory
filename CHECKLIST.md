# Cursor Implementation Checklist

**Project: FoundationDataFactory (FDF)**

> ⚠️ **IMPORTANT CONSTRAINTS (DO NOT VIOLATE)**
>
> * Public data abstraction = **Hugging Face Dataset only**
> * Input = Dataset, Output = Dataset
> * **Daft is REQUIRED** for execution
> * **Ray is REQUIRED** for distributed / GPU execution
> * Stage is the ONLY checkpoint boundary
> * Do NOT invent a new Dataset or DataFrame abstraction
> * Do NOT expose Daft or Ray in public APIs

---

## PR0 — Repository Scaffold (NO LOGIC)

**Goal:** repo installs, CLI exists, nothing else

### Tasks

* [ ] Create repo layout:

  ```text
  foundation-data-factory/
    README.md
    DESIGN.md
    pyproject.toml
    src/fdf/
      __init__.py
      cli/
        main.py
  ```

* [ ] Define console script:

  ```toml
  [project.scripts]
  fdf = "fdf.cli.main:main"
  ```

* [ ] `fdf --help` prints usage
* [ ] `fdf version` prints version

### Hard rules

* ❌ No pipeline logic
* ❌ No Dataset code
* ❌ No Daft/Ray yet

---

## PR1 — YAML Schema & Validation (NO EXECUTION)

**Goal:** parse + validate pipeline.yaml, fail loudly

### Tasks

* [ ] `src/fdf/config/schema.py` (pydantic)

  * `PipelineConfig`
  * `InputConfig`
  * `MixtureConfig`
  * `StageConfig`
  * `OperatorInstanceConfig`
* [ ] Validate:

  * stage names unique
  * operator ids unique per stage
  * operator.kind ∈ {score,evaluator,filter,refiner,generator}
* [ ] CLI:

  ```bash
  fdf validate pipeline.yaml
  ```

* [ ] Tests:

  * valid pipeline parses
  * duplicate stage fails
  * duplicate operator id fails

### Hard rules

* ❌ No execution
* ❌ No Daft
* ❌ No Ray

---

## PR2 — Operator Registry & Contracts

**Goal:** define what an operator *is*

### Tasks

* [ ] `src/fdf/operators/base.py`

  * abstract base:

    * `BatchOperator`
    * metadata: name, version, kind
* [ ] `src/fdf/operators/registry.py`

  * `@register_operator`
  * `get_operator_class(name)`
* [ ] Define operator metadata:

  * name
  * version
  * kind
  * optional paper reference
* [ ] Implement **one trivial operator**:

  * `PassthroughRefiner` (kind=refiner)

### Tests

* [ ] operator registration works
* [ ] duplicate registration fails

### Hard rules

* ❌ Operator must NOT see HF Dataset
* ❌ Operator input = MicroPartition (in-place modification, returns None)
* ❌ Operator must NOT collect data to driver - work on partitions distributedly

---

## PR3 — Dataset Boundary Enforcement

**Goal:** lock in “Dataset in → Dataset out” forever

### Tasks

* [ ] `src/fdf/api.py`

  ```python
  def run_pipeline(
      dataset: datasets.Dataset | datasets.IterableDataset,
      pipeline_yaml: str,
      *,
      run_dir: str | None = None,
  ) -> datasets.Dataset
  ```

* [ ] Type-check input is HF Dataset
* [ ] Return value is HF Dataset
* [ ] Reject anything else

### Tests

* [ ] passing list / pandas fails
* [ ] passing Dataset works

### Hard rules

* ❌ No custom Dataset class
* ❌ No Ray/Daft visible here

---

## PR4 — Local Runtime (Daft REQUIRED)

**Goal:** execute 1 stage locally using Daft

### Tasks

* [ ] `src/fdf/runtime/executor.py`
* [ ] Convert HF Dataset → Arrow batches
* [ ] Convert Arrow → Daft DataFrame
* [ ] Apply operators sequentially
* [ ] Convert back → Arrow → HF Dataset
* [ ] Write Parquet shards
* [ ] Emit `manifest.json`

### Tests

* [ ] input parquet → output parquet
* [ ] manifest written
* [ ] output is HF Dataset

### Hard rules

* ❌ No pandas fallback
* ❌ Must import and use `daft`
* ❌ Must not expose Daft in API

---

## PR5 — Stage Materialization & Resume

**Goal:** stage-level fault tolerance

### Tasks

* [ ] `src/fdf/lineage/manifest.py`
* [ ] Stage writes:

  * shard files
  * manifest.json
* [ ] Resume logic:

  * incremental: skip completed shards
  * overwrite: rerun
* [ ] Config:

  ```yaml
  materialize:
    path: ...
    mode: incremental | overwrite
  ```

### Tests

* [ ] rerun skips completed shards
* [ ] overwrite reruns everything

### Hard rules

* ❌ No operator-level checkpoints
* ❌ Resume ONLY at stage level

---

## PR6 — Data Mixture (Input Side)

**Goal:** dataset-level mixture, streaming-first

### Tasks

* [ ] `src/fdf/mixture/input_mixture.py`
* [ ] Support:

  * multiple HF datasets
  * weights
  * streaming interleave
* [ ] No global shuffle
* [ ] Track consumption counters

### Tests

* [ ] mixture respects weights (approx)
* [ ] streaming datasets supported

### Hard rules

* ❌ No materialize-before-mix
* ❌ No full scan for ratios

---

---

## PR9 — Ray Runtime (CPU First)

**Goal:** distributed execution

### Tasks

* [ ] `src/fdf/runtime/ray_runtime.py`
* [ ] Ray init/shutdown
* [ ] Dataset shards → Ray tasks
* [ ] Parallel stage execution
* [ ] Merge stats → manifest

### Tests

* [ ] local Ray cluster
* [ ] deterministic output

### Hard rules

* ❌ No Ray Dataset abstraction
* ❌ Ray only schedules work

---

## PR10 — GPU Actor Pool

**Goal:** GPU operators scale correctly

### Tasks

* [ ] `src/fdf/runtime/actor_pool.py`
* [ ] Long-lived actors
* [ ] Load model once
* [ ] Process Arrow batches
* [ ] Resource-aware scheduling

### Tests

* [ ] mock GPU operator
* [ ] actor reuse verified

### Hard rules

* ❌ No per-batch model load
* ❌ GPU ops MUST run in actors

---

## PR11 — Predefined Pipelines & CLI UX

**Goal:** usable project

### Tasks

* [ ] `pipelines/` directory
* [ ] `fdf pipelines list`
* [ ] `fdf pipelines show <name>`
* [ ] Example pipelines:

  * refinedweb-like
  * laion captioning

---

## PR12 — Final Guardrails

**Must be true before merge**

* [ ] HF Dataset in/out enforced everywhere
* [ ] Daft used in execution path
* [ ] Ray used for parallelism
* [ ] No custom Dataset abstraction
* [ ] Stage = only checkpoint
* [ ] Manifest written for every stage
* [ ] Streaming path works

---

## Cursor Prompt Footer (IMPORTANT)

> ⚠️ Do not invent new abstractions.
> ⚠️ Follow DESIGN.md exactly.
> ⚠️ Implement only the current PR scope.
> ⚠️ Write tests for every PR.
> ⚠️ Ask for clarification instead of guessing.

---
