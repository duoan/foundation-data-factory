from __future__ import annotations

import json
import textwrap

import datasets as hfds

from fdf.config.schema import PipelineConfig
from fdf.runtime.executor import run_stage_ray


def _make_dataset(data: list[dict]) -> hfds.Dataset:
    return hfds.Dataset.from_dict({k: [d[k] for d in data] for k in data[0]})


def _make_stage() -> PipelineConfig:
    yaml_str = textwrap.dedent(
        """
        name: "ray-test"
        stages:
          - name: "stage-a"
            input:
              type: "mixture"
            operators:
              - id: "refine.passthrough"
                kind: "refiner"
                op: "passthrough-refiner"
            output:
              source:
                    type: "parquet"
                    path: "/tmp/test-output"
        """
    ).strip()
    return PipelineConfig.from_yaml_str(yaml_str)


def test_ray_runtime_processes_dataset_shards(tmp_path):
    """Test that Ray runtime processes dataset shards in parallel."""
    import daft

    from fdf.config.schema import DataSourceConfig, OutputConfig
    from fdf.io.reader import read_data_source

    pipeline = _make_stage()
    stage = pipeline.stages[0]
    # Set output path for this test
    stage.output = OutputConfig(source=DataSourceConfig(type="parquet", path=str(tmp_path / "output")))
    # Create a larger dataset that will be split into shards
    dataset = _make_dataset([{"x": i, "y": i * 2} for i in range(10)])

    input_df = daft.from_arrow(dataset.data.table)
    run_stage_ray(stage, input_df=input_df)

    # Read output back to verify
    result_df = read_data_source(stage.output.source).collect()
    result = hfds.Dataset.from_dict(result_df.to_pydict())

    assert isinstance(result, hfds.Dataset)
    assert len(result) == 10
    assert list(result["x"]) == list(range(10))
    assert list(result["y"]) == [i * 2 for i in range(10)]


def test_ray_runtime_deterministic_output(tmp_path):
    """Test that Ray runtime produces deterministic output."""
    import daft

    from fdf.config.schema import DataSourceConfig, OutputConfig
    from fdf.io.reader import read_data_source

    pipeline = _make_stage()
    stage = pipeline.stages[0]
    # Set output path for this test
    stage.output = OutputConfig(source=DataSourceConfig(type="parquet", path=str(tmp_path / "output")))
    dataset = _make_dataset([{"x": 1}, {"x": 2}, {"x": 3}])

    input_df = daft.from_arrow(dataset.data.table)
    run_stage_ray(stage, input_df=input_df)

    # Read output back to verify
    result_df = read_data_source(stage.output.source).collect()
    result = hfds.Dataset.from_dict(result_df.to_pydict())

    assert list(result["x"]) == [1, 2, 3]


def test_ray_runtime_with_output(tmp_path):
    """Test Ray runtime with stage output."""
    import daft

    from fdf.io.reader import read_data_source

    yaml_str = textwrap.dedent(
        f"""
        name: "ray-output-test"
        stages:
          - name: "stage-a"
            input:
              type: "mixture"
            operators:
              - id: "refine.passthrough"
                kind: "refiner"
                op: "passthrough-refiner"
            output:
              source:
                type: "parquet"
                path: "{tmp_path.as_posix()}"
        """
    ).strip()
    pipeline = PipelineConfig.from_yaml_str(yaml_str)
    stage = pipeline.stages[0]
    dataset = _make_dataset([{"x": 1}, {"x": 2}])

    input_df = daft.from_arrow(dataset.data.table)
    run_stage_ray(stage, input_df=input_df)

    # Read output back to verify
    result_df = read_data_source(stage.output.source).collect()
    result = hfds.Dataset.from_dict(result_df.to_pydict())

    assert isinstance(result, hfds.Dataset)
    assert list(result["x"]) == [1, 2]

    manifest_path = tmp_path / "manifest.json"
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text())
    assert manifest["stage"] == "stage-a"
    assert manifest["output_rows"] == 2
    assert manifest["path"] == str(tmp_path.as_posix())
