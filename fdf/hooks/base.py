from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa


class Hook(ABC):
    """Base class for runtime hooks."""

    @abstractmethod
    def on_stage_start(self, stage_name: str) -> None:
        """Called when a stage starts."""

    @abstractmethod
    def on_partition_end(self, table: pa.Table) -> None:
        """Called after a partition/batch is processed."""

    @abstractmethod
    def on_stage_end(self, result: Any) -> None:
        """Called after the stage finishes."""
