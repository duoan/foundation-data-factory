from __future__ import annotations

from typing import Any

import daft
from daft.expressions import ExpressionsProjection, col
from daft.functions import get
from daft.recordbatch.micropartition import MicroPartition
from textstat import textstat

from fdf.operators.registry import register_operator

from .base import BatchOperator

# Default metrics configuration (from example code)
# Only include metrics that have corresponding functions in _default_metric_functions
_default_metrics = {
    "flesch_reading_ease": {"min": 0, "max": 100},
    "automated_readability_index": {"min": 0, "max": 100},
    "syllable_count": {"min": 32.0, "max": 2331.9},
    "lexicon_count": {"min": 23.0, "max": 1554.0},
    "sentence_count": {"min": 1.0, "max": 89.1},
    "character_count": {"min": 118.0, "max": 7466.3},
    "letter_count": {"min": 109.0, "max": 7193.0},
    "polysyllable_count": {"min": 0.0, "max": 216.4},
    "monosyllable_count": {"min": 13.0, "max": 1044.1},
    "difficult_words": {"min": 4.0, "max": 213.4},
}

_default_metric_functions = {
    "flesch_reading_ease": textstat.flesch_reading_ease,
    "automated_readability_index": textstat.automated_readability_index,
    "text_standard": textstat.text_standard,
    "syllable_count": textstat.syllable_count,
    "lexicon_count": textstat.lexicon_count,
    "sentence_count": textstat.sentence_count,
    "character_count": textstat.char_count,  # char_count not character_count
    "letter_count": textstat.letter_count,
    "polysyllable_count": textstat.polysyllabcount,  # polysyllabcount not polysyllable_count
    "monosyllable_count": textstat.monosyllabcount,  # monosyllabcount not monosyllable_count
    "difficult_words": textstat.difficult_words,
    "difficult_words_count": textstat.difficult_words,
}


@register_operator
class TextstatFilter(BatchOperator):
    """Filter data based on textstat text statistics scores.

    Uses textstat to calculate text statistics and filters rows where all metrics
    fall within specified min/max thresholds.

    Parameters are set during initialization:
    - column (str): Column name to process. Defaults to "text".
    - metrics (dict): Dictionary mapping metric names to their min/max thresholds.
                    Each metric should have "min" and "max" keys.
                    Example: {"flesch_reading_ease": {"min": 0.0, "max": 100.0}}
                    Defaults to all available metrics with predefined thresholds.

    Available metrics:
    - flesch_reading_ease
    - automated_readability_index
    - syllable_count
    - lexicon_count
    - sentence_count
    - character_count
    - letter_count
    - polysyllable_count
    - monosyllable_count
    - difficult_words

    Reference: https://github.com/shivam5992/textstat
    """

    name = "textstat-filter"
    version = "0.0.1"
    kind = "filter"
    paper = "https://github.com/shivam5992/textstat"

    def __init__(
        self,
        *,
        column: str = "text",
        metrics: dict[str, dict[str, float]] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize TextstatFilter with parameters.

        Args:
            column: Column name to process. Defaults to "text".
            metrics: Dictionary mapping metric names to their min/max thresholds.
                    Each metric can have optional "min" and/or "max" keys.
                    Missing values will use defaults from _default_metrics.
                    Example: {"flesch_reading_ease": {"min": 0.0, "max": 100.0}}
                    Or: {"flesch_reading_ease": {}} to use defaults
                    Defaults to all available metrics with predefined thresholds.
            **kwargs: Additional parameters (ignored for now, reserved for future use).
        """
        self.column = column
        self.metrics = self._process_metrics(metrics)
        self.metric_functions = {
            k: _default_metric_functions[k] for k in self.metrics if k in _default_metric_functions
        }
        self.text_metrics_udf = self._create_text_metrics_udf(self.metric_functions)

    def _process_metrics(self, metrics: dict[str, dict[str, float]] | None) -> dict[str, dict[str, float]]:
        """Process and validate metrics configuration.

        Args:
            metrics: User-provided metrics configuration, or None for defaults.

        Returns:
            Validated metrics dictionary with merged thresholds.
        """
        if metrics is None:
            return _default_metrics.copy()

        processed_metrics = {}
        for metric_name, thresholds in metrics.items():
            processed_metrics[metric_name] = self._merge_metric_thresholds(metric_name, thresholds)

        if not processed_metrics:
            msg = "No valid metrics found"
            raise ValueError(msg)

        return processed_metrics

    def _merge_metric_thresholds(self, metric_name: str, thresholds: dict[str, float] | None) -> dict[str, float]:
        """Merge user-provided thresholds with defaults for a single metric.

        Args:
            metric_name: Name of the metric.
            thresholds: User-provided thresholds (can be None or empty dict).

        Returns:
            Merged thresholds dictionary with min and max values.
        """
        # Validate metric name
        if metric_name not in _default_metric_functions:
            msg = f"Unknown metric '{metric_name}'. Available metrics: {list(_default_metric_functions.keys())}"
            raise ValueError(msg)

        # Get default thresholds
        default_thresholds = _default_metrics.get(metric_name, {"min": 0.0, "max": 100.0})
        merged_thresholds = default_thresholds.copy()

        # Merge with provided thresholds if any
        if thresholds:
            if not isinstance(thresholds, dict):
                msg = f"Metric '{metric_name}' thresholds must be a dict with optional 'min' and 'max' keys"
                raise TypeError(msg)

            if "min" in thresholds:
                if not isinstance(thresholds["min"], (int, float)):
                    msg = f"Metric '{metric_name}' min must be a number"
                    raise TypeError(msg)
                merged_thresholds["min"] = float(thresholds["min"])

            if "max" in thresholds:
                if not isinstance(thresholds["max"], (int, float)):
                    msg = f"Metric '{metric_name}' max must be a number"
                    raise TypeError(msg)
                merged_thresholds["max"] = float(thresholds["max"])

        return merged_thresholds

    def _create_text_metrics_udf(self, metric_functions: dict[str, Any]) -> Any:
        """Create a UDF for computing text metrics.

        Args:
            metric_functions: Dictionary mapping metric names to their functions.

        Returns:
            A Daft UDF function for computing text metrics.
        """
        return_struct = daft.DataType.struct({name: daft.DataType.float64() for name in metric_functions})

        @daft.func(return_dtype=return_struct)
        def text_metrics(text: str | None) -> dict[str, float | None]:
            """Compute text metrics for a single text string."""
            if text is None:
                return dict.fromkeys(metric_functions)

            row = {}
            for k, fn in metric_functions.items():
                try:
                    v = fn(text)
                    row[k] = float(v) if v is not None else None
                except Exception:
                    row[k] = None
            return row

        return text_metrics

    def apply(self, mp: MicroPartition) -> None:
        """Filter data based on textstat metrics in-place.

        Args:
            mp: Daft MicroPartition to modify in-place
        """
        # Check if column exists
        column_names = mp.column_names()
        if self.column not in column_names:
            msg = f"Column '{self.column}' not found in MicroPartition. Available columns: {column_names}"
            raise ValueError(msg)

        # New column
        proj = ExpressionsProjection(
            [col(c) for c in mp.column_names()] + [self.text_metrics_udf(col(self.column)).alias("text_metrics")],
        )

        # Filters
        predicate = None
        for m, thresholds in self.metrics.items():
            lo = float(thresholds["min"])
            hi = float(thresholds["max"])
            metric_col = get(col("text_metrics"), m)
            cond = metric_col.not_null() & metric_col.between(lo, hi)
            predicate = cond if predicate is None else (predicate & cond)

        out = mp.eval_expression_list(proj).filter(ExpressionsProjection([predicate]))

        # Update the MicroPartition in-place by replacing its internal micropartition
        mp._micropartition = out._micropartition
