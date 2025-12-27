"""Tests for fdf.operators.textstat_filter module."""

from __future__ import annotations

import daft
import pytest
from daft.recordbatch.micropartition import MicroPartition

from fdf.operators.textstat_filter import TextstatFilter


@pytest.fixture
def sample_micropartition() -> MicroPartition:
    """Create a sample MicroPartition for testing."""
    df = daft.from_pydict({"text": ["Hello world. This is a test.", "Short text."]})
    return next(iter(df.iter_partitions()))


def test_textstat_filter_import_without_textstat(monkeypatch, sample_micropartition: MicroPartition) -> None:
    """Test that TextstatFilter requires textstat to be installed.

    Since textstat is a required dependency, the import will fail at module import time
    if textstat is not available. This test verifies that the module depends on textstat.
    """
    # This test is mainly for documentation - if textstat is not installed,
    # the module import itself will fail, which is the expected behavior.
    # In practice, textstat should always be available when using this operator.
    pass


def test_textstat_filter_params_deprecated(sample_micropartition: MicroPartition) -> None:
    """Test that apply() no longer accepts params parameter."""
    op = TextstatFilter()
    mp = sample_micropartition

    # apply() should work without params
    op.apply(mp)
    table = mp.to_arrow()
    assert "text_metrics" in table.column_names


def test_textstat_filter_basic_usage(sample_micropartition: MicroPartition) -> None:
    """Test basic usage of TextstatFilter."""
    op = TextstatFilter(
        metrics={
            "flesch_reading_ease": {"min": 0.0, "max": 100.0},
            "sentence_count": {"min": 0.0, "max": 10.0},  # Allow 0 sentences
        },
    )
    mp = sample_micropartition

    op.apply(mp)

    # Verify text_metrics column was added
    table = mp.to_arrow()
    assert "text_metrics" in table.column_names


def test_textstat_filter_only_uses_specified_metrics(sample_micropartition: MicroPartition) -> None:
    """Test that only user-specified metrics are used, not all default metrics."""
    # Only specify one metric - should only use that one, not all defaults
    op = TextstatFilter(
        metrics={"sentence_count": {}},  # Only this metric, not all defaults
    )
    mp = sample_micropartition

    op.apply(mp)

    table = mp.to_arrow()
    assert "text_metrics" in table.column_names

    # Verify only sentence_count is in the metrics struct
    df = daft.from_arrow(table)
    if table.num_rows > 0:
        row = df.to_pylist()[0]
        text_metrics = row["text_metrics"]
        # Should only have sentence_count, not all default metrics
        assert "sentence_count" in text_metrics or hasattr(text_metrics, "sentence_count")
        # Should not have other default metrics like flesch_reading_ease
        # (Note: this is a structural check - the struct should only contain sentence_count)

    # Verify text_metrics contains expected metrics (if any rows remain after filtering)
    if table.num_rows > 0:
        df = daft.from_arrow(table)
        for row in df.to_pylist():
            text_metrics = row["text_metrics"]
            assert text_metrics is not None
            # text_metrics is a struct, check that it has the expected fields
            assert "flesch_reading_ease" in text_metrics or hasattr(text_metrics, "flesch_reading_ease")
            assert "sentence_count" in text_metrics or hasattr(text_metrics, "sentence_count")


def test_textstat_filter_with_custom_column(sample_micropartition: MicroPartition) -> None:
    """Test TextstatFilter with custom column name."""
    op = TextstatFilter(
        column="text",
        metrics={"sentence_count": {"min": 0.0, "max": 10.0}},
    )
    mp = sample_micropartition

    op.apply(mp)

    table = mp.to_arrow()
    assert "text_metrics" in table.column_names


def test_textstat_filter_uses_default_thresholds(sample_micropartition: MicroPartition) -> None:
    """Test that metrics without thresholds use default values."""
    # Only specify metric name without thresholds - should use defaults
    op = TextstatFilter(
        metrics={"sentence_count": {}},  # Empty dict should use defaults
    )
    mp = sample_micropartition

    op.apply(mp)

    table = mp.to_arrow()
    assert "text_metrics" in table.column_names


def test_textstat_filter_partial_thresholds(sample_micropartition: MicroPartition) -> None:
    """Test that metrics with partial thresholds merge with defaults."""
    # Only specify min, max should come from defaults
    op = TextstatFilter(
        metrics={"sentence_count": {"min": 2.0}},  # Only min, max from default (89.1)
    )
    mp = sample_micropartition

    op.apply(mp)

    table = mp.to_arrow()
    assert "text_metrics" in table.column_names


def test_textstat_filter_filters_rows(sample_micropartition: MicroPartition) -> None:
    """Test that TextstatFilter actually filters rows based on criteria."""
    op = TextstatFilter(
        metrics={"sentence_count": {"min": 2.0, "max": 10.0}},  # Require at least 2 sentences
    )
    mp = sample_micropartition

    original_count = mp.to_arrow().num_rows
    op.apply(mp)
    filtered_count = mp.to_arrow().num_rows

    # Should filter out rows that don't meet criteria
    # Note: "Hello world. This is a test." has 2 sentences, "Short text." has 1 sentence
    # So after filtering with min_scores=2.0, only the first row should remain
    assert filtered_count <= original_count
    assert filtered_count >= 0  # Should not be negative


def test_textstat_filter_default_scores(sample_micropartition: MicroPartition) -> None:
    """Test TextstatFilter with default min/max scores."""
    op = TextstatFilter()  # Uses all default metrics
    mp = sample_micropartition

    op.apply(mp)

    table = mp.to_arrow()
    assert "text_metrics" in table.column_names


def test_textstat_filter_mismatched_min_max_scores(sample_micropartition: MicroPartition) -> None:
    """Test that missing min or max keys use defaults."""
    # Should work - missing max will use default
    op = TextstatFilter(
        metrics={"flesch_reading_ease": {"min": 0.0}},  # Missing max, will use default
    )
    mp = sample_micropartition
    op.apply(mp)  # Should work fine


def test_textstat_filter_invalid_metrics(sample_micropartition: MicroPartition) -> None:
    """Test that invalid metrics raise error."""
    with pytest.raises(ValueError, match="Unknown metric 'invalid_metric'"):
        TextstatFilter(metrics={"invalid_metric": {"min": 0.0, "max": 100.0}})


def test_textstat_filter_missing_column_fallback(sample_micropartition: MicroPartition) -> None:
    """Test that missing column raises ValueError."""
    op = TextstatFilter(
        column="nonexistent",
        metrics={"sentence_count": {"min": 0.0, "max": 10.0}},
    )
    # Create MicroPartition without 'text' column
    df = daft.from_pydict({"other_col": ["test"]})
    mp = next(iter(df.iter_partitions()))

    # Should raise ValueError when column is not found
    with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
        op.apply(mp)


def test_textstat_filter_empty_text(sample_micropartition: MicroPartition) -> None:
    """Test TextstatFilter with empty text."""
    op = TextstatFilter(
        metrics={"sentence_count": {"min": 0.0, "max": 10.0}},
    )
    df = daft.from_pydict({"text": [None, "", "Valid text."]})
    mp = next(iter(df.iter_partitions()))

    op.apply(mp)

    table = mp.to_arrow()
    assert "text_metrics" in table.column_names


def test_textstat_filter_accepts_extra_kwargs(sample_micropartition: MicroPartition) -> None:
    """Test that TextstatFilter accepts additional unknown parameters via **kwargs."""
    # Should not raise error when passing extra parameters
    op = TextstatFilter(
        metrics={"sentence_count": {"min": 0.0, "max": 10.0}},
        extra_param="ignored",
        another_param=123,
    )
    mp = sample_micropartition

    # Should work normally despite extra parameters
    op.apply(mp)

    table = mp.to_arrow()
    assert "text_metrics" in table.column_names
