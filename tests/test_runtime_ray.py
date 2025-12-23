from __future__ import annotations

import json
import textwrap

import datasets as hfds

from fdf.config.schema import PipelineConfig
from fdf.runtime.ray_runtime import run_stage_ray


def _make_dataset(data: list[dict]) -> hfds.Dataset:
    return hfds.Dataset.from_dict({k: [d[k] for d in data] for k in data[0]})


def _make_stage() -> PipelineConfig:
    yaml_str = textwrap.dedent(
        """
        name: "ray-test"
        input:
          type: "mixture"
        stages:
          - name: "stage-a"
            operators:
              - id: "refine.passthrough"
                kind: "refiner"
                op: "passthrough-refiner"
        output:
          type: "parquet"
        """
    ).strip()
    return PipelineConfig.from_yaml_str(yaml_str)


def test_ray_runtime_processes_dataset_shards(tmp_path):
    """Test that Ray runtime processes dataset shards in parallel."""
    pipeline = _make_stage()
    stage = pipeline.stages[0]
    # Create a larger dataset that will be split into shards
    dataset = _make_dataset([{"x": i, "y": i * 2} for i in range(10)])

    result = run_stage_ray(dataset, stage)

    assert isinstance(result, hfds.Dataset)
    assert len(result) == 10
    assert list(result["x"]) == list(range(10))
    assert list(result["y"]) == [i * 2 for i in range(10)]


def test_ray_runtime_deterministic_output(tmp_path):
    """Test that Ray runtime produces deterministic output."""
    pipeline = _make_stage()
    stage = pipeline.stages[0]
    dataset = _make_dataset([{"x": 1}, {"x": 2}, {"x": 3}])

    result1 = run_stage_ray(dataset, stage)
    result2 = run_stage_ray(dataset, stage)

    assert list(result1["x"]) == list(result2["x"])
    assert list(result1["x"]) == [1, 2, 3]


def test_ray_runtime_with_materialization(tmp_path):
    """Test Ray runtime with stage materialization."""
    yaml_str = textwrap.dedent(
        f"""
        name: "ray-materialize-test"
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
              mode: "overwrite"
        output:
          type: "parquet"
        """
    ).strip()
    pipeline = PipelineConfig.from_yaml_str(yaml_str)
    stage = pipeline.stages[0]
    dataset = _make_dataset([{"x": 1}, {"x": 2}])

    result = run_stage_ray(dataset, stage)

    assert isinstance(result, hfds.Dataset)
    assert list(result["x"]) == [1, 2]

    manifest_path = tmp_path / "manifest.json"
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text())
    assert manifest["stage"] == "stage-a"
    assert manifest["output_rows"] == 2
    assert len(manifest["shards"]) > 0  # Should have at least one shard
