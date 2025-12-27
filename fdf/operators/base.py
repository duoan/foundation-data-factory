from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from daft.recordbatch.micropartition import MicroPartition


class BatchView:
    """High-level view of a batch that hides Daft implementation details.

    This abstraction allows operators to work with fields (columns) without
    needing to know about Daft's MicroPartition, ExpressionsProjection, etc.

    Operators should use this view to:
    - Read field values
    - Add new fields
    - Filter rows based on conditions
    - Remove fields (for refiner operations)
    """

    def __init__(self, mp: MicroPartition) -> None:
        """Initialize BatchView with a MicroPartition.

        Args:
            mp: Daft MicroPartition to wrap
        """
        self._mp = mp

    def column_names(self) -> list[str]:
        """Get list of column names in the batch.

        Returns:
            List of column names
        """
        return self._mp.column_names()

    def has_column(self, name: str) -> bool:
        """Check if a column exists.

        Args:
            name: Column name to check

        Returns:
            True if column exists, False otherwise
        """
        return name in self.column_names()

    def add_column_from_udf(
        self,
        input_column: str,
        udf_func: Callable[[Any], Any],
        output_column: str,
        return_dtype: Any | None = None,
        *,
        already_decorated: bool = False,
    ) -> None:
        """Add a new column by applying a UDF to an existing column.

        Args:
            input_column: Name of the input column to process
            output_column: Name of the new column to create
            udf_func: UDF function to apply. If already_decorated is False, will be wrapped with @daft.func
            return_dtype: Optional return type for the UDF (if None, will be inferred). Ignored if already_decorated is True.
            already_decorated: If True, udf_func is already decorated with @daft.func and will be used as-is

        Raises:
            ValueError: If input_column doesn't exist
        """
        import daft
        from daft.expressions import ExpressionsProjection, col

        if not self.has_column(input_column):
            available = self.column_names()
            msg = f"Column '{input_column}' not found. Available columns: {available}"
            raise ValueError(msg)

        # Use the function as-is if already decorated, otherwise wrap it
        if already_decorated:
            daft_udf = udf_func
        else:
            daft_udf = (
                daft.func(return_dtype=return_dtype)(udf_func) if return_dtype is not None else daft.func(udf_func)
            )

        # Create projection with all existing columns plus the new one
        proj = ExpressionsProjection(
            [col(c) for c in self.column_names()] + [daft_udf(col(input_column)).alias(output_column)],
        )

        # Evaluate and update
        self._mp._micropartition = self._mp.eval_expression_list(proj)._micropartition

    def filter_rows(self, condition_func: Callable[[Any], Any], condition_column: str) -> None:
        """Filter rows based on a condition applied to a column.

        Args:
            condition_func: Function that takes a column value and returns a boolean expression
                          (should return a Daft expression, not a Python bool)
            condition_column: Name of the column to apply the condition to

        Raises:
            ValueError: If condition_column doesn't exist
        """
        from daft.expressions import ExpressionsProjection, col

        if not self.has_column(condition_column):
            available = self.column_names()
            msg = f"Column '{condition_column}' not found. Available columns: {available}"
            raise ValueError(msg)

        # Apply condition function to get a boolean expression
        condition = condition_func(col(condition_column))

        # Filter and update
        filtered = self._mp.filter(ExpressionsProjection([condition]))
        self._mp._micropartition = filtered._micropartition

    def filter_rows_by_struct_field(
        self,
        struct_column: str,
        field_name: str,
        min_value: float | None = None,
        max_value: float | None = None,
        require_not_null: bool = True,
    ) -> None:
        """Filter rows based on a field within a struct column.

        This is a convenience method for common filtering patterns where you have
        a struct column and want to filter based on one of its fields.

        Args:
            struct_column: Name of the struct column
            field_name: Name of the field within the struct
            min_value: Optional minimum value (inclusive)
            max_value: Optional maximum value (inclusive)
            require_not_null: If True, filter out null values

        Raises:
            ValueError: If struct_column doesn't exist
        """
        from daft.expressions import ExpressionsProjection, col
        from daft.functions import get

        if not self.has_column(struct_column):
            available = self.column_names()
            msg = f"Column '{struct_column}' not found. Available columns: {available}"
            raise ValueError(msg)

        # Get the field from the struct
        field_col = get(col(struct_column), field_name)

        # Build condition
        condition = None
        if require_not_null:
            condition = field_col.not_null()

        if min_value is not None:
            min_cond = field_col >= min_value
            condition = min_cond if condition is None else (condition & min_cond)

        if max_value is not None:
            max_cond = field_col <= max_value
            condition = max_cond if condition is None else (condition & max_cond)

        if condition is None:
            # No filtering needed
            return

        # Filter and update
        filtered = self._mp.filter(ExpressionsProjection([condition]))
        self._mp._micropartition = filtered._micropartition

    def filter_rows_by_multiple_conditions(
        self,
        conditions: list[tuple[str, str, float | None, float | None]],
        require_all: bool = True,
    ) -> None:
        """Filter rows based on multiple struct field conditions.

        Args:
            conditions: List of (struct_column, field_name, min_value, max_value) tuples
            require_all: If True, all conditions must be met (AND). If False, any condition (OR).

        Example:
            conditions = [
                ("metrics", "score1", 0.0, 100.0),
                ("metrics", "score2", 10.0, None),
            ]
        """
        from daft.expressions import ExpressionsProjection, col
        from daft.functions import get

        if not conditions:
            return

        predicates = []
        for struct_column, field_name, min_value, max_value in conditions:
            if not self.has_column(struct_column):
                available = self.column_names()
                msg = f"Column '{struct_column}' not found. Available columns: {available}"
                raise ValueError(msg)

            field_col = get(col(struct_column), field_name)
            cond = field_col.not_null()

            if min_value is not None:
                cond = cond & (field_col >= min_value)
            if max_value is not None:
                cond = cond & (field_col <= max_value)

            predicates.append(cond)

        # Combine predicates
        if require_all:
            # AND: all must be true
            combined = predicates[0]
            for p in predicates[1:]:
                combined = combined & p
        else:
            # OR: any can be true
            combined = predicates[0]
            for p in predicates[1:]:
                combined = combined | p

        # Filter and update
        filtered = self._mp.filter(ExpressionsProjection([combined]))
        self._mp._micropartition = filtered._micropartition

    def remove_column(self, name: str) -> None:
        """Remove a column from the batch.

        Args:
            name: Name of the column to remove

        Raises:
            ValueError: If column doesn't exist
        """
        from daft.expressions import ExpressionsProjection, col

        if not self.has_column(name):
            available = self.column_names()
            msg = f"Column '{name}' not found. Available columns: {available}"
            raise ValueError(msg)

        # Create projection without the removed column
        remaining_cols = [c for c in self.column_names() if c != name]
        proj = ExpressionsProjection([col(c) for c in remaining_cols])

        # Evaluate and update
        self._mp._micropartition = self._mp.eval_expression_list(proj)._micropartition

    def get_micropartition(self) -> MicroPartition:
        """Get the underlying MicroPartition (for advanced use cases).

        This should only be used when the high-level API is insufficient.
        Prefer using the other methods in this class.

        Returns:
            The underlying MicroPartition
        """
        return self._mp


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

    Operators should use BatchView to interact with data, which hides Daft implementation
    details and provides a field-centric API.
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
    def apply(self, batch: BatchView, params: dict[str, Any] | None = None) -> None:
        """Apply the operator to a batch in-place.

        Args:
            batch: BatchView to modify in-place (wraps MicroPartition)
            params: Optional dictionary of operator-specific parameters from config

        Implementations must:
        - modify the batch in-place using BatchView methods
        - not return anything (None)
        - use BatchView API instead of directly accessing MicroPartition
        - use params to configure behavior (e.g., column names, thresholds)

        Example:
            def apply(self, batch: BatchView, params: dict[str, Any] | None = None) -> None:
                # Add a new column
                batch.add_column_from_udf("text", my_udf, "output")

                # Filter rows
                batch.filter_rows_by_struct_field("metrics", "score", min_value=0.5)
        """
