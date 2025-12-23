from __future__ import annotations

from typing import Any

from daft.recordbatch.micropartition import MicroPartition

from .base import BatchOperator
from .registry import register_operator


@register_operator
class PassthroughRefiner(BatchOperator):
    """Trivial refiner operator that returns the input batch unchanged.

    This is primarily used as a smoke-test for the operator contracts and
    registry mechanism.
    """

    name = "passthrough-refiner"
    version = "0.0.1"
    kind = "refiner"

    def apply(self, batch: MicroPartition, params: dict[str, Any] | None = None) -> None:
        """Passthrough: no-op, batch unchanged."""
        pass
