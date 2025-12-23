from __future__ import annotations

import json
import textwrap
from pathlib import Path

import datasets as hfds

from fdf.config.schema import PipelineConfig
from fdf.runtime.executor import run_stage_local


def _stage_with_output(tmp_path: Path):
    yaml_str = textwrap.dedent(
        f"""
        name: "output-test"
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
    return PipelineConfig.from_yaml_str(yaml_str).stages[0]


def _dataset(values) -> hfds.Dataset:
    return hfds.Dataset.from_dict({"x": list(values)})


def test_output_writes_manifest_and_parquet(tmp_path):
    import daft

    from fdf.io.reader import read_data_source

    stage = _stage_with_output(tmp_path)
    dataset = _dataset([1, 2, 3])

    input_df = daft.from_arrow(dataset.data.table)
    run_stage_local(stage, input_df=input_df)

    # Read output back to verify
    result_df = read_data_source(stage.output.source).collect()
    result = hfds.Dataset.from_dict(result_df.to_pydict())

    manifest = tmp_path / "manifest.json"
    # Output path should exist (parquet files)
    output_files = list(tmp_path.glob("*.parquet"))

    assert manifest.exists()
    assert len(output_files) > 0
    assert list(result["x"]) == [1, 2, 3]

    manifest_data = json.loads(manifest.read_text())
    assert manifest_data["stage"] == "stage-a"
    assert manifest_data["output_rows"] == 3
    assert manifest_data["path"] == str(tmp_path.as_posix())


def test_output_can_be_read_from_previous_stage(tmp_path):
    import daft

    from fdf.config.schema import InputConfig
    from fdf.io.reader import read_data_source

    stage = _stage_with_output(tmp_path)
    first = _dataset([1, 2, 3])

    # First run writes output
    input_df1 = daft.from_arrow(first.data.table)
    run_stage_local(stage, input_df=input_df1)

    # Second run should read from previous stage's output path
    # Configure input to read from the output path
    stage.input = InputConfig(source=stage.output.source)
    run_stage_local(stage)

    # Read final output to verify
    result_df = read_data_source(stage.output.source).collect()
    result = hfds.Dataset.from_dict(result_df.to_pydict())
    assert list(result["x"]) == [1, 2, 3]
