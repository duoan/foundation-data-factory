from __future__ import annotations

import textwrap

import datasets as hfds

from fdf.api import run_pipeline, run_pipeline_from_yaml
from fdf.config.schema import PipelineConfig


def _write_minimal_pipeline(tmp_path, input_type: str = "parquet", input_path: str | None = None) -> str:
    path = tmp_path / "pipeline.yaml"
    output_path = tmp_path / "output"
    if input_path is None:
        input_path = str(tmp_path / "input")
    path.write_text(
        textwrap.dedent(
            f"""
            name: "api-test"
            stages:
              - name: "stage-a"
                input:
                  source:
                    type: "{input_type}"
                    path: "{input_path}"
                operators: []
                output:
                  source:
                    type: "parquet"
                    path: "{output_path}"
            """
        ).strip()
    )
    return str(path)


def test_run_pipeline_from_config(tmp_path):
    """Test that run_pipeline accepts PipelineConfig and executes stages."""

    output_path = tmp_path / "output"
    # Create a simple test dataset instead of downloading from Hugging Face
    test_data = hfds.Dataset.from_dict({"text": ["hello", "world"]})

    config = PipelineConfig.from_yaml_str(
        textwrap.dedent(
            f"""
            name: "test"
            stages:
              - name: "stage1"
                input:
                  source:
                    type: "parquet"
                    path: "{tmp_path / "input"}"
                operators: []
                output:
                  source:
                    type: "parquet"
                    path: "{output_path}"
            """
        ).strip()
    )

    # Write input data first
    input_path = tmp_path / "input"
    input_path.mkdir()
    import pyarrow.parquet as pq

    pq.write_table(test_data.data.table, input_path / "input.parquet")

    result = run_pipeline(config)

    assert isinstance(result, hfds.Dataset)
    assert len(result) > 0


def test_run_pipeline_from_yaml(tmp_path):
    """Test backward compatibility: run_pipeline_from_yaml."""
    import pyarrow.parquet as pq

    # Create input data
    test_data = hfds.Dataset.from_dict({"text": ["hello", "world"]})
    input_path = tmp_path / "input"
    input_path.mkdir()
    pq.write_table(test_data.data.table, input_path / "input.parquet")

    pipeline_path = _write_minimal_pipeline(tmp_path, input_type="parquet", input_path=str(input_path))

    result = run_pipeline_from_yaml(pipeline_path)

    assert isinstance(result, hfds.Dataset)
    assert len(result) > 0


def test_run_pipeline_with_mixture_input(tmp_path):
    """Test pipeline with mixture input (currently not implemented)."""
    import pyarrow.parquet as pq
    import pytest

    # Create test datasets
    ds1 = hfds.Dataset.from_dict({"text": ["a", "b"]})
    ds2 = hfds.Dataset.from_dict({"text": ["c", "d"]})

    input1_path = tmp_path / "input1"
    input2_path = tmp_path / "input2"
    input1_path.mkdir()
    input2_path.mkdir()
    pq.write_table(ds1.data.table, input1_path / "data.parquet")
    pq.write_table(ds2.data.table, input2_path / "data.parquet")

    output_path = tmp_path / "output"
    config = PipelineConfig.from_yaml_str(
        textwrap.dedent(
            f"""
            name: "mixture-test"
            stages:
              - name: "stage1"
                input:
                  type: "mixture"
                  seed: 42
                  datasets:
                    - name: "ds1"
                      source:
                        type: "parquet"
                        path: "{input1_path}"
                      weight: 0.5
                    - name: "ds2"
                      source:
                        type: "parquet"
                        path: "{input2_path}"
                      weight: 0.5
                operators: []
                output:
                  source:
                    type: "parquet"
                    path: "{output_path}"
            """
        ).strip()
    )

    # Mixture input is not yet implemented
    with pytest.raises(NotImplementedError, match="Mixture input not yet implemented"):
        run_pipeline(config)
