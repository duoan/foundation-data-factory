from __future__ import annotations

from pathlib import Path
from typing import Any

import datasets as hfds

from fdf.config.schema import PipelineConfig


class NotHFDatasetError(TypeError):
    """Raised when the input is not a Hugging Face Dataset or IterableDataset."""

    def __init__(self) -> None:
        super().__init__("dataset must be a Hugging Face Dataset or IterableDataset")


def _ensure_hf_dataset(dataset: Any) -> None:
    """Validate that the provided object is a Hugging Face Dataset."""

    if not isinstance(dataset, (hfds.Dataset, hfds.IterableDataset)):
        raise NotHFDatasetError()


def run_pipeline(
    dataset: hfds.Dataset | hfds.IterableDataset,
    pipeline_yaml: str | Path,
    *,
    run_dir: str | None = None,
) -> hfds.Dataset | hfds.IterableDataset:
    """Public API: Dataset in → Dataset out.

    This enforces the HF Dataset boundary. Execution is intentionally
    stubbed for PR3; later PRs will implement actual runtime behavior.
    """

    _ensure_hf_dataset(dataset)

    # Validate the pipeline configuration; for PR3 we don't execute it.
    PipelineConfig.from_yaml_file(str(pipeline_yaml))

    return dataset
