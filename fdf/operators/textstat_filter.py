from __future__ import annotations

import json
from typing import Any

import daft
from daft.recordbatch.micropartition import MicroPartition

from .base import BatchOperator

# Check if textstat is available at module import time
try:
    import textstat as textstat_lib  # type: ignore[import-untyped]

    _TEXTSTAT_AVAILABLE = True
except ImportError:
    _TEXTSTAT_AVAILABLE = False
    textstat_lib = None  # type: ignore[assignment]


def _register_if_available(cls: type[BatchOperator]) -> type[BatchOperator]:
    """Register operator only if dependencies are available."""
    if _TEXTSTAT_AVAILABLE:
        from .registry import register_operator

        return register_operator(cls)
    return cls


@_register_if_available
class TextstatFilter(BatchOperator):
    """Filter data based on textstat text statistics scores.

    Uses textstat to calculate text statistics and filters rows where all metrics
    fall within specified min/max thresholds.

    Parameters:
        - column (str, optional): Column name to process. Defaults to "text".
        - min_scores (dict, optional): Minimum thresholds for each metric.
        - max_scores (dict, optional): Maximum thresholds for each metric.
        - metrics (list[str], optional): List of metrics to use for filtering.
          Available metrics:
          - flesch_reading_ease
          - automated_readability_index
          - aggregate_reading_level
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

    def apply(self, batch: MicroPartition, params: dict[str, Any] | None = None) -> None:
        """Filter data based on textstat metrics in-place.

        Args:
            batch: Daft MicroPartition to modify in-place
            params: Optional parameters dict with 'column', 'min_scores', 'max_scores', 'metrics'
        """
        if not _TEXTSTAT_AVAILABLE:
            msg = "textstat is required for TextstatFilter. Install with: pip install textstat"
            raise ImportError(msg)

        params = params or {}
        column_name = params.get("column", "text")

        # Default min/max scores (from example code)
        default_min_scores = {
            "flesch_reading_ease": 0,
            "automated_readability_index": 0,
            "aggregate_reading_level": 0,
            "syllable_count": 32.0,
            "lexicon_count": 23.0,
            "sentence_count": 1.0,
            "character_count": 118.0,
            "letter_count": 109.0,
            "polysyllable_count": 0.0,
            "monosyllable_count": 13.0,
            "difficult_words": 4.0,
        }

        default_max_scores = {
            "flesch_reading_ease": 100,
            "automated_readability_index": 100,
            "aggregate_reading_level": 100,
            "syllable_count": 2331.9,
            "lexicon_count": 1554.0,
            "sentence_count": 89.1,
            "character_count": 7466.3,
            "letter_count": 7193.0,
            "polysyllable_count": 216.4,
            "monosyllable_count": 1044.1,
            "difficult_words": 213.4,
        }

        min_scores = params.get("min_scores", default_min_scores)
        max_scores = params.get("max_scores", default_max_scores)
        requested_metrics = params.get("metrics", list(default_min_scores.keys()))

        # Validate that min_scores and max_scores have the same keys
        if set(min_scores.keys()) != set(max_scores.keys()):
            msg = "min_scores and max_scores must have the same keys"
            raise ValueError(msg)

        # Metric name mapping to textstat functions
        # Use the underlying textstat library directly (not langkit.textstat wrapper)
        # Support both "difficult_words" and "difficult_words_count" for compatibility
        textstat_obj = textstat_lib.textstat  # type: ignore[attr-defined]

        # Helper function for aggregate_reading_level (text_standard with float_output)
        def aggregate_reading_level(text: str) -> float:
            return float(textstat_obj.text_standard(text, float_output=True))

        metric_functions = {
            "flesch_reading_ease": textstat_obj.flesch_reading_ease,
            "automated_readability_index": textstat_obj.automated_readability_index,
            "aggregate_reading_level": aggregate_reading_level,
            "syllable_count": textstat_obj.syllable_count,
            "lexicon_count": textstat_obj.lexicon_count,
            "sentence_count": textstat_obj.sentence_count,
            "character_count": textstat_obj.char_count,  # char_count not character_count
            "letter_count": textstat_obj.letter_count,
            "polysyllable_count": textstat_obj.polysyllabcount,  # polysyllabcount not polysyllable_count
            "monosyllable_count": textstat_obj.monosyllabcount,  # monosyllabcount not monosyllable_count
            "difficult_words": textstat_obj.difficult_words,
            "difficult_words_count": textstat_obj.difficult_words,
        }

        # Filter to only requested metrics
        metric_functions = {m: metric_functions[m] for m in requested_metrics if m in metric_functions}

        if not metric_functions:
            msg = f"No valid metrics found in {requested_metrics}"
            raise ValueError(msg)

        # Convert MicroPartition to DataFrame for Daft UDF processing
        table = batch.to_arrow()
        df = daft.from_arrow(table)

        # Check if column exists
        if column_name not in df.column_names:
            # Fallback to 'text' if specified column doesn't exist
            if "text" in df.column_names:
                column_name = "text"
            else:
                # No text columns found, nothing to do
                return

        # Calculate all metrics at once and create metadata JSON field
        df = self._calculate_all_metrics(df, column_name, metric_functions, operator_name=self.name)

        # Build filter conditions from metadata JSON
        filter_conditions = self._build_filter_conditions_from_metadata(df, metric_functions, min_scores, max_scores)

        # Combine all conditions with AND
        combined_filter = filter_conditions[0]
        for condition in filter_conditions[1:]:
            combined_filter = combined_filter & condition

        # Apply filter
        df = df.filter(combined_filter)

        # Convert back to MicroPartition
        result_partitions = list(df.iter_partitions())
        if result_partitions:
            batch._micropartition = result_partitions[0]._micropartition

    def _calculate_all_metrics(
        self,
        df: daft.DataFrame,
        column_name: str,
        metric_functions: dict[str, Any],
        operator_name: str,
    ) -> daft.DataFrame:
        """Calculate all textstat metrics at once and create metadata JSON field.

        Uses a single UDF to compute all metrics, avoiding multiple passes over the text.
        Only creates a 'metadata' JSON column, no individual metric columns.
        """
        metric_names = list(metric_functions.keys())
        metric_funcs = list(metric_functions.values())

        # Create a UDF that calculates all metrics at once
        def make_all_metrics_udf(funcs: list[Any], m_names: list[str], op_name: str) -> Any:
            @daft.func(return_dtype=daft.DataType.string())
            def all_metrics_udf(text: str | None) -> str:
                """Calculate all metrics for a single text and return as JSON."""
                if text is None or not isinstance(text, str):
                    metrics_dict = dict.fromkeys(m_names, 0.0)
                else:
                    metrics_dict = {}
                    for metric_name, metric_func in zip(m_names, funcs):
                        try:
                            value = metric_func(text)
                            metrics_dict[metric_name] = float(value) if value is not None else 0.0
                        except Exception:
                            metrics_dict[metric_name] = 0.0

                metadata = {op_name: metrics_dict}
                return json.dumps(metadata)

            return all_metrics_udf

        # Apply UDF to calculate all metrics and create metadata column
        all_metrics_udf = make_all_metrics_udf(metric_funcs, metric_names, operator_name)
        df = df.with_column("metadata", all_metrics_udf(df[column_name]))

        return df

    def _build_filter_conditions_from_metadata(
        self,
        df: daft.DataFrame,
        metric_functions: dict[str, Any],
        min_scores: dict[str, float],
        max_scores: dict[str, float],
    ) -> list[Any]:
        """Build filter conditions by extracting metrics from metadata JSON."""
        operator_name = self.name
        metric_names = list(metric_functions.keys())

        # Create UDFs to extract each metric from metadata JSON for filtering
        filter_conditions = []
        for metric_name in metric_names:
            min_score = min_scores.get(metric_name, float("-inf"))
            max_score = max_scores.get(metric_name, float("inf"))

            # Create UDF to extract specific metric from metadata JSON
            def make_extract_metric_udf(m_name: str, op_name: str) -> Any:
                @daft.func(return_dtype=daft.DataType.float64())
                def extract_metric_udf(metadata_json: str) -> float:
                    """Extract a specific metric value from metadata JSON."""
                    try:
                        metadata = json.loads(metadata_json)
                        if op_name in metadata and m_name in metadata[op_name]:
                            return float(metadata[op_name][m_name])
                    except Exception:  # noqa: S110
                        # Silently handle JSON parsing errors or missing keys
                        pass
                    return 0.0

                return extract_metric_udf

            extract_udf = make_extract_metric_udf(metric_name, operator_name)
            metric_col = extract_udf(df["metadata"])

            # Build filter condition for this metric
            condition = (metric_col >= min_score) & (metric_col <= max_score)  # type: ignore[operator]
            filter_conditions.append(condition)

        return filter_conditions
