from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import daft
import datasets as hfds
import pyarrow as pa
import pyarrow.parquet as pq

from fdf.config.schema import StageConfig
from fdf.hooks.base import Hook
from fdf.operators.registry import get_operator_class

try:
    import ray
except ImportError:
    ray = None  # type: ignore[assignment, misc]


class RayNotAvailableError(ImportError):
    """Raised when Ray is not available."""

    def __init__(self) -> None:
        super().__init__("Ray is required for ray_runtime. Install with: pip install ray")


def _ensure_ray_available() -> None:
    """Ensure Ray is available, raise ImportError if not."""
    if ray is None:
        raise RayNotAvailableError()


def _ensure_dataset(dataset: Any) -> hfds.Dataset:
    """Ensure the dataset is a non-iterable Dataset."""
    if not isinstance(dataset, hfds.Dataset):
        msg = "only supports datasets.Dataset"
        raise TypeError(msg)
    return dataset


@ray.remote  # type: ignore[misc]
def _process_shard(
    shard_data: list[dict],
    stage: StageConfig,
) -> pa.Table:
    """Process a single shard of data using the stage operators."""
    table: pa.Table = pa.Table.from_pylist(shard_data)
    daft.from_arrow(table)  # Enforce Daft usage

    for op_cfg in stage.operators:
        op_name = getattr(op_cfg, "op", None)
        if not op_name:
            continue
        op_cls = get_operator_class(op_name)
        op = op_cls()
        table = op.apply(table)
        daft.from_arrow(table)  # Enforce Daft usage

    return table


def _split_dataset_into_shards(dataset: hfds.Dataset, num_shards: int) -> list[list[dict]]:
    """Split dataset into shards for parallel processing."""
    data_list = list(dataset)
    shard_size = max(1, len(data_list) // num_shards)
    shards: list[list[dict]] = []
    for i in range(0, len(data_list), shard_size):
        shards.append(data_list[i : i + shard_size])
    return shards


def _materialize_stage_ray(
    result_tables: list[pa.Table],
    combined_table: pa.Table,
    stage: StageConfig,
    hooks: list[Hook],
) -> hfds.Dataset | None:
    """Materialize stage results to disk and return loaded dataset.

    Returns None if materialization is not configured.
    """
    materialize = stage.materialize
    if not materialize:
        return None

    base = Path(materialize.path)
    base.mkdir(parents=True, exist_ok=True)
    manifest_path = base / "manifest.json"
    shard_paths: list[str] = []

    if materialize.mode == "incremental" and manifest_path.exists():
        existing_manifest = json.loads(manifest_path.read_text())
        existing_shards = existing_manifest.get("shards", [])
        if existing_shards and all(Path(p).exists() for p in existing_shards):
            result = hfds.Dataset.from_parquet(existing_shards)
            for hook in hooks:
                hook.on_stage_end(result)
            return result

    for idx, table in enumerate(result_tables):
        shard_path = base / f"data_shard_{idx}.parquet"
        pq.write_table(table, shard_path)
        shard_paths.append(shard_path.as_posix())

    hook_artifacts: list[dict] = []
    for hook in hooks:
        if hasattr(hook, "manifest_artifacts"):
            artifacts = hook.manifest_artifacts()
            if artifacts:
                hook_artifacts.append(artifacts)

    manifest = {
        "stage": stage.name,
        "output_rows": combined_table.num_rows,
        "shards": shard_paths,
        "hook_artifacts": hook_artifacts,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))

    result = hfds.Dataset.from_parquet(shard_paths)
    for hook in hooks:
        hook.on_stage_end(result)
    return result


def run_stage_ray(
    dataset: hfds.Dataset,
    stage: StageConfig,
    *,
    hooks: list[Hook] | None = None,
    num_shards: int = 4,
) -> hfds.Dataset:
    """Run a stage using Ray for distributed execution.

    Args:
        dataset: Input Hugging Face Dataset (non-iterable)
        stage: Stage configuration
        hooks: Optional list of hooks for observability
        num_shards: Number of shards to split the dataset into for parallel processing

    Returns:
        Output Hugging Face Dataset
    """
    _ensure_ray_available()
    dataset = _ensure_dataset(dataset)
    hooks = hooks or []

    # Initialize Ray if not already initialized
    if not ray.is_initialized():  # type: ignore[misc]
        ray.init(ignore_reinit_error=True)  # type: ignore[misc]

    try:
        for hook in hooks:
            hook.on_stage_start(stage.name)

        shards = _split_dataset_into_shards(dataset, num_shards)
        futures = [_process_shard.remote(shard, stage) for shard in shards]  # type: ignore[misc]
        result_tables = ray.get(futures)  # type: ignore[misc]

        combined_table = pa.concat_tables(result_tables)

        for hook in hooks:
            hook.on_partition_end(combined_table)

        materialized = _materialize_stage_ray(result_tables, combined_table, stage, hooks)
        if materialized is not None:
            return materialized

        output_ds = hfds.Dataset.from_dict(combined_table.to_pydict())
        for hook in hooks:
            hook.on_stage_end(output_ds)
        return output_ds

    finally:
        # Don't shutdown Ray here as it might be used by other code
        # The caller is responsible for managing Ray lifecycle
        pass
