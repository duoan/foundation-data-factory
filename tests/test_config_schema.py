from __future__ import annotations

import textwrap

import pytest

from fdf.config.schema import (
    OperatorInstanceConfig,
    PipelineConfig,
    StageConfig,
)


def make_minimal_pipeline_yaml() -> str:
    """Return a minimal but valid pipeline YAML string for tests."""

    return textwrap.dedent(
        """
        name: "test-pipeline"

        stages:
          - name: "stage-a"
            input:
              type: "mixture"
            operators:
              - id: "score.text_quality"
                kind: "score"
            output:
              source:
                type: "parquet"
                path: "/tmp/test-output"
        """
    ).strip()


def test_pipeline_config_validates_minimal_pipeline():
    yaml_str = make_minimal_pipeline_yaml()
    cfg = PipelineConfig.from_yaml_str(yaml_str)

    assert cfg.name == "test-pipeline"
    assert len(cfg.stages) == 1
    stage = cfg.stages[0]
    assert stage.name == "stage-a"
    assert len(stage.operators) == 1
    op = stage.operators[0]
    assert op.id == "score.text_quality"
    assert op.kind == "score"


def test_stage_names_must_be_unique():
    yaml_str = textwrap.dedent(
        """
        name: "test-duplicate-stages"

        stages:
          - name: "dup-stage"
            input:
              type: "mixture"
            operators: []
            output:
              source:
                type: "parquet"
                path: "/tmp/test-output"
          - name: "dup-stage"
            input:
              type: "mixture"
            operators: []
            output:
              source:
                type: "parquet"
                path: "/tmp/test-output"
        """
    ).strip()

    with pytest.raises(ValueError, match="Stage names must be unique"):
        PipelineConfig.from_yaml_str(yaml_str)


def test_operator_ids_must_be_unique_within_stage():
    yaml_str = textwrap.dedent(
        """
        name: "test-duplicate-ops"

        stages:
          - name: "stage-a"
            input:
              type: "mixture"
            operators:
              - id: "op-1"
                kind: "score"
              - id: "op-1"
                kind: "filter"
            output:
              source:
                type: "parquet"
                path: "/tmp/test-output"
        """
    ).strip()

    with pytest.raises(ValueError, match="Operator ids must be unique per stage"):
        PipelineConfig.from_yaml_str(yaml_str)


@pytest.mark.parametrize(
    "kind",
    ["score", "evaluator", "filter", "refiner", "generator"],
)
def test_operator_kind_accepts_allowed_values(kind: str):
    OperatorInstanceConfig(id="test-op", kind=kind)


def test_operator_kind_must_be_valid():
    with pytest.raises(ValueError, match="Invalid operator kind"):
        OperatorInstanceConfig(id="bad-op", kind="invalid-kind")


def test_stage_config_requires_unique_operator_ids(tmp_path):
    operators = [
        OperatorInstanceConfig(id="dup", kind="score"),
        OperatorInstanceConfig(id="dup", kind="score"),
    ]

    from fdf.config.schema import DataSourceConfig, OutputConfig

    with pytest.raises(ValueError, match="Operator ids must be unique per stage"):
        StageConfig(
            name="stage-a",
            operators=operators,
            output=OutputConfig(source=DataSourceConfig(type="parquet", path=str(tmp_path / "test"))),
        )
