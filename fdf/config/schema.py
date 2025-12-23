from __future__ import annotations

from collections.abc import Sequence

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

    @field_validator("kind")
    @classmethod
    def validate_kind(cls, value: str) -> str:
        if value not in ALLOWED_OPERATOR_KINDS:
            msg = f"Invalid operator kind: {value!r}. Allowed kinds: {sorted(ALLOWED_OPERATOR_KINDS)}"
            raise ValueError(msg)
        return value


class StageConfig(BaseModel):
    """Configuration for a single stage in the pipeline."""

    name: str
    operators: list[OperatorInstanceConfig] = Field(default_factory=list)
    materialize: MaterializeConfig | None = None

    @model_validator(mode="after")
    def ensure_unique_operator_ids(self) -> StageConfig:
        ids = [op.id for op in self.operators]
        if len(ids) != len(set(ids)):
            raise OperatorIdsNotUniqueError()
        return self


class InputConfig(BaseModel):
    """Top-level input configuration.

    For PR1 we keep this intentionally minimal and permissive. Additional
    fields from the design/README can be added in later PRs.
    """

    type: str


class OutputConfig(BaseModel):
    """Top-level output configuration."""

    type: str


class PipelineConfig(BaseModel):
    """Top-level pipeline configuration model."""

    name: str
    input: InputConfig
    stages: list[StageConfig] = Field(default_factory=list)
    output: OutputConfig

    @model_validator(mode="after")
    def ensure_unique_stage_names(self) -> PipelineConfig:
        names: Sequence[str] = [stage.name for stage in self.stages]
        if len(names) != len(set(names)):
            raise StageNamesNotUniqueError()
        return self

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


class MaterializeConfig(BaseModel):
    """Stage materialization configuration."""

    path: str
    mode: str = "incremental"  # incremental | overwrite


class InvalidMaterializeModeError(ValueError):
    """Raised when an unsupported materialize mode is provided."""

    def __init__(self, mode: str) -> None:
        super().__init__(f"Invalid materialize.mode: {mode}. Use 'incremental' or 'overwrite'.")

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, value: str) -> str:
        if value not in {"incremental", "overwrite"}:
            raise InvalidMaterializeModeError(value)
        return value


StageConfig.model_rebuild()
