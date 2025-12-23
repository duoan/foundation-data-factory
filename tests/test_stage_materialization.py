from __future__ import annotations

import json
import textwrap
from pathlib import Path

import datasets as hfds

from fdf.config.schema import PipelineConfig
from fdf.runtime.local import run_stage_local


def _stage_with_materialize(tmp_path: Path, mode: str = "incremental"):
    yaml_str = textwrap.dedent(
        f"""
        name: "mat-test"
        input:
          type: "mixture"
        stages:
          - name: "stage-a"
            operators:
              - id: "refine.passthrough"
                kind: "refiner"
                op: "passthrough-refiner"
            materialize:
              path: "{tmp_path.as_posix()}"
              mode: "{mode}"
        output:
          type: "parquet"
        """
    ).strip()
    return PipelineConfig.from_yaml_str(yaml_str).stages[0]


def _dataset(values) -> hfds.Dataset:
    return hfds.Dataset.from_dict({"x": list(values)})


def test_materialize_writes_manifest_and_parquet(tmp_path):
    stage = _stage_with_materialize(tmp_path)
    dataset = _dataset([1, 2, 3])

    result = run_stage_local(dataset, stage)

    manifest = tmp_path / "manifest.json"
    shard = tmp_path / "data.parquet"

    assert manifest.exists()
    assert shard.exists()
    assert list(result["x"]) == [1, 2, 3]

    manifest_data = json.loads(manifest.read_text())
    assert manifest_data["stage"] == "stage-a"
    assert manifest_data["output_rows"] == 3
    assert shard.as_posix() in manifest_data["shards"]


def test_incremental_skips_when_manifest_exists(tmp_path):
    stage = _stage_with_materialize(tmp_path, mode="incremental")
    first = _dataset([1, 2, 3])
    second = _dataset([9, 9, 9])

    # First run writes output
    run_stage_local(first, stage)

    # Second run should skip and keep original data
    result = run_stage_local(second, stage)
    assert list(result["x"]) == [1, 2, 3]


def test_overwrite_reruns_even_if_manifest_exists(tmp_path):
    stage_inc = _stage_with_materialize(tmp_path, mode="incremental")
    stage_overwrite = _stage_with_materialize(tmp_path, mode="overwrite")

    first = _dataset([1, 2, 3])
    second = _dataset([7, 8])

    run_stage_local(first, stage_inc)

    result = run_stage_local(second, stage_overwrite)

    assert list(result["x"]) == [7, 8]
