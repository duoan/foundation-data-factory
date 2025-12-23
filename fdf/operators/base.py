from __future__ import annotations

from abc import ABC, abstractmethod

import pyarrow as pa


class BatchOperator(ABC):
    """Abstract base class for all batch operators.

    Important constraints (from DESIGN/CHECKLIST):

    - Operators must operate on **columnar / Arrow batches**, not on
      Hugging Face Datasets directly.
    - Public operator API is batch-in → batch-out.
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
    def apply(self, batch: pa.Table) -> pa.Table:
        """Apply the operator to a single Arrow batch.

        Implementations must:
        - treat the input batch as immutable
        - return a new or reused Arrow table representing the output batch
        """
