from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from daft.recordbatch.micropartition import MicroPartition


class BatchOperator(ABC):
    """Abstract base class for all batch operators.

    Important constraints (from DESIGN/CHECKLIST):

    - Operators must operate on **columnar batches**, not on Hugging Face Datasets directly.
    - Public operator API is batch-in → batch-out.
    - Operators work with Daft's internal representation (MicroPartition) to avoid
      unnecessary conversions when executing within Daft's data plane.

    Design rationale:
    - Since Daft is the internal data plane, operators should work directly with
      Daft's MicroPartition to avoid MicroPartition <-> Arrow Table conversions.
    - Arrow Table is still supported for compatibility with external systems,
      but MicroPartition is preferred for Daft-native execution.
    """

    #: Human-readable operator name, used for registration.
    name: str

    #: Semantic version of the operator implementation.
    version: str

    #: Kind of the operator: score | evaluator | filter | refiner | generator
    kind: str

    #: Optional paper or reference string.
    paper: str | None = None

    @abstractmethod
    def apply(self, part: MicroPartition, params: dict[str, Any] | None = None) -> None:
        """Apply the operator to a batch in-place.

        Args:
            batch: Daft's MicroPartition to modify in-place
            params: Optional dictionary of operator-specific parameters from config

        Implementations must:
        - modify the batch in-place
        - not return anything (None)
        - work directly with MicroPartition to avoid conversions
        - use params to configure behavior (e.g., column names, thresholds)
        """
