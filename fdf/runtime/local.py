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


def _ensure_dataset(dataset: Any) -> hfds.Dataset:
    if not isinstance(dataset, hfds.Dataset):
        msg = "only supports datasets.Dataset"
        raise TypeError(msg)
    return dataset


def _materialize_if_configured(table: pa.Table, stage: StageConfig, hooks: list[Hook]) -> hfds.Dataset | None:
    materialize = stage.materialize
    if not materialize:
        return None

    base = Path(materialize.path)
    base.mkdir(parents=True, exist_ok=True)
    manifest_path = base / "manifest.json"
    shard_path = base / "data.parquet"

    if materialize.mode == "incremental" and manifest_path.exists() and shard_path.exists():
        result = hfds.Dataset.from_parquet(shard_path.as_posix())
        for hook in hooks:
            hook.on_stage_end(result)
        return result

    pq.write_table(table, shard_path)

    result = hfds.Dataset.from_parquet(shard_path.as_posix())
    for hook in hooks:
        hook.on_stage_end(result)

    hook_artifacts: list[str] = []
    for hook in hooks:
        hook_artifacts.extend(getattr(hook, "artifacts", []))

    manifest = {
        "stage": stage.name,
        "output_rows": table.num_rows,
        "shards": [shard_path.as_posix()],
        "hook_artifacts": hook_artifacts,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))

    return result


def run_stage_local(
    dataset: hfds.Dataset,
    stage: StageConfig,
    *,
    hooks: list[Hook] | None = None,
) -> hfds.Dataset:
    dataset = _ensure_dataset(dataset)
    hooks = hooks or []

    for hook in hooks:
        hook.on_stage_start(stage.name)

    table: pa.Table = pa.Table.from_pylist(list(dataset))
    daft.from_arrow(table)

    for op_cfg in stage.operators:
        op_name = getattr(op_cfg, "op", None)
        if not op_name:
            continue
        op_cls = get_operator_class(op_name)
        op = op_cls()
        table = op.apply(table)
        daft.from_arrow(table)

    output_ds = hfds.Dataset.from_dict(table.to_pydict())

    for hook in hooks:
        hook.on_partition_end(table)

    materialized = _materialize_if_configured(table, stage, hooks)
    if materialized is not None:
        return materialized

    for hook in hooks:
        hook.on_stage_end(output_ds)
    return output_ds
