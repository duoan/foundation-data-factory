from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

ALLOWED_OPERATOR_KINDS = {"score", "evaluator", "filter", "refiner", "generator"}


class OperatorIdsNotUniqueError(ValueError):
    """Raised when operator ids are not unique within a stage."""

    def __init__(self) -> None:
        super().__init__("Operator ids must be unique per stage")


class StageNamesNotUniqueError(ValueError):
    """Raised when stage names are not unique within a pipeline."""

    def __init__(self) -> None:
        super().__init__("Stage names must be unique")


class OperatorInstanceConfig(BaseModel):
    """Configuration for a single operator instance within a stage."""

    id: str
    kind: str
    op: str | None = None
    params: dict[str, Any] | None = None  # Operator-specific parameters

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: str) -> str:
        if value not in ALLOWED_OPERATOR_KINDS:
            msg = f"Invalid operator kind: {value!r}. Allowed kinds: {sorted(ALLOWED_OPERATOR_KINDS)}"
            raise ValueError(msg)
        return value


class StageConfig(BaseModel):
    """Configuration for a single stage in the pipeline.

    Each stage has:
    - input: Input configuration (required for first stage, auto-configured from previous stage's output for others)
    - output: Output configuration (required - every stage must materialize results)
    """

    name: str
    operators: list[OperatorInstanceConfig] = Field(default_factory=list)
    input: InputConfig | None = None  # Required for first stage, auto-configured for others
    output: OutputConfig  # Required - every stage must materialize results

    @model_validator(mode="after")
    def ensure_unique_operator_ids(self) -> StageConfig:
        ids = [op.id for op in self.operators]
        if len(ids) != len(set(ids)):
            raise OperatorIdsNotUniqueError()
        return self


class DataSourceConfig(BaseModel):
    """Unified data source configuration for input and output.

    Supports multiple data formats via Daft connectors:
    - parquet: Parquet files (local or S3)
    - json: JSON files
    - csv: CSV files
    - iceberg: Apache Iceberg tables
    - lance: Lance format
    - huggingface: Hugging Face datasets
    """

    type: str  # parquet, json, csv, iceberg, lance, huggingface
    path: str | None = None  # Path/URI to data source

    # Hugging Face specific options
    streaming: bool = False  # For type="huggingface"
    token: str | None = None  # For type="huggingface" (private datasets)

    # Iceberg specific options
    catalog: str | None = None  # For type="iceberg"
    table: str | None = None  # For type="iceberg"

    @field_validator("type")
    @classmethod
    def validate_type(cls, value: str) -> str:
        allowed_types = {"parquet", "json", "csv", "iceberg", "lance", "huggingface"}
        if value not in allowed_types:
            msg = f"Invalid data source type: {value!r}. Allowed types: {sorted(allowed_types)}"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def validate_path_required(self) -> DataSourceConfig:
        """Validate that path is provided for most types."""
        if self.type == "iceberg":
            if self.catalog is None or self.table is None:
                msg = "catalog and table are required for type='iceberg'"
                raise ValueError(msg)
        elif self.path is None:
            msg = f"path is required for type='{self.type}'"
            raise ValueError(msg)
        return self


class DatasetSourceConfig(BaseModel):
    """Configuration for a single dataset source in a mixture."""

    name: str
    source: DataSourceConfig
    weight: float


class InputConfig(BaseModel):
    """Input configuration.

    Supports:
    - Single data source: use DataSourceConfig directly
    - Mixture: weighted combination of multiple data sources
    """

    # For single data source
    type: str | None = None  # If None, use source field
    source: DataSourceConfig | None = None

    # For mixture
    mixture: list[DatasetSourceConfig] | None = None
    seed: int | None = None  # For mixture randomization

    @model_validator(mode="after")
    def validate_input_config(self) -> InputConfig:
        """Validate that either source or mixture is provided."""
        if self.mixture is not None:
            # Mixture mode
            if self.source is not None or self.type is not None:
                msg = "Cannot specify both mixture and source/type"
                raise ValueError(msg)
            if not self.mixture:
                msg = "mixture must contain at least one dataset source"
                raise ValueError(msg)
        else:
            # Single source mode
            if self.source is None:
                if self.type is None:
                    msg = "Either source or mixture must be provided"
                    raise ValueError(msg)
                # Backward compatibility: create source from type and path
                # This will be handled by the reader
                pass
        return self


class OutputConfig(BaseModel):
    """Output configuration.

    Uses DataSourceConfig for unified data source handling.
    """

    source: DataSourceConfig


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration model.

    Pipeline-level input/output are derived from:
    - First stage's input config (pipeline input)
    - Last stage's output config (pipeline output)
    """

    name: str
    stages: list[StageConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def ensure_unique_stage_names(self) -> PipelineConfig:
        names: Sequence[str] = [stage.name for stage in self.stages]
        if len(names) != len(set(names)):
            raise StageNamesNotUniqueError()
        return self

    def get_pipeline_input(self) -> InputConfig:
        """Get pipeline input config from first stage."""
        if not self.stages:
            msg = "Pipeline has no stages"
            raise ValueError(msg)
        first_stage = self.stages[0]
        if first_stage.input is None:
            msg = f"First stage '{first_stage.name}' must have input configuration"
            raise ValueError(msg)
        return first_stage.input

    def get_pipeline_output(self) -> OutputConfig:
        """Get pipeline output config from last stage."""
        if not self.stages:
            msg = "Pipeline has no stages"
            raise ValueError(msg)
        return self.stages[-1].output

    @classmethod
    def from_yaml_str(cls, content: str) -> PipelineConfig:
        """Parse and validate a pipeline configuration from a YAML string."""

        data = yaml.safe_load(content) or {}
        try:
            return cls.model_validate(data)
        except ValidationError as exc:
            # For now we raise ValueError from here to keep the tests and CLI
            # layer simple. Later PRs can introduce richer error types.
            raise ValueError(str(exc)) from exc

    @classmethod
    def from_yaml_file(cls, path: str) -> PipelineConfig:
        """Parse and validate a pipeline configuration from a YAML file."""

        with open(path, encoding="utf-8") as f:
            content = f.read()
        return cls.from_yaml_str(content)


StageConfig.model_rebuild()
