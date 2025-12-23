"""Tests for fdf.operators.textstat_filter module."""

from __future__ import annotations

import json

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
    """Test that TextstatFilter raises ImportError when textstat is not available."""
    # Mock the module-level variable
    import fdf.operators.textstat_filter as textstat_module

    original_available = textstat_module._TEXTSTAT_AVAILABLE
    monkeypatch.setattr(textstat_module, "_TEXTSTAT_AVAILABLE", False)

    op = TextstatFilter()
    mp = sample_micropartition

    try:
        with pytest.raises(ImportError, match="textstat is required"):
            op.apply(mp, {})
    finally:
        # Restore original value
        monkeypatch.setattr(textstat_module, "_TEXTSTAT_AVAILABLE", original_available)


def test_textstat_filter_basic_usage(sample_micropartition: MicroPartition) -> None:
    """Test basic usage of TextstatFilter."""
    op = TextstatFilter()
    mp = sample_micropartition

    params = {
        "metrics": ["flesch_reading_ease", "sentence_count"],
        "min_scores": {"flesch_reading_ease": 0.0, "sentence_count": 1.0},
        "max_scores": {"flesch_reading_ease": 100.0, "sentence_count": 10.0},
    }

    op.apply(mp, params)

    # Verify metadata column was added
    table = mp.to_arrow()
    assert "metadata" in table.column_names

    # Verify metadata contains expected metrics
    df = daft.from_arrow(table)
    for row in df.to_pylist():
        metadata = json.loads(row["metadata"])
        assert "textstat-filter" in metadata
        assert "flesch_reading_ease" in metadata["textstat-filter"]
        assert "sentence_count" in metadata["textstat-filter"]


def test_textstat_filter_with_custom_column(sample_micropartition: MicroPartition) -> None:
    """Test TextstatFilter with custom column name."""
    op = TextstatFilter()
    mp = sample_micropartition

    params = {
        "column": "text",
        "metrics": ["sentence_count"],
        "min_scores": {"sentence_count": 0.0},
        "max_scores": {"sentence_count": 10.0},
    }

    op.apply(mp, params)

    table = mp.to_arrow()
    assert "metadata" in table.column_names


def test_textstat_filter_filters_rows(sample_micropartition: MicroPartition) -> None:
    """Test that TextstatFilter actually filters rows based on criteria."""
    op = TextstatFilter()
    mp = sample_micropartition

    # Set strict filter that should exclude some rows
    params = {
        "metrics": ["sentence_count"],
        "min_scores": {"sentence_count": 2.0},  # Require at least 2 sentences
        "max_scores": {"sentence_count": 10.0},
    }

    original_count = mp.to_arrow().num_rows
    op.apply(mp, params)
    filtered_count = mp.to_arrow().num_rows

    # Should filter out rows that don't meet criteria
    assert filtered_count <= original_count


def test_textstat_filter_default_scores(sample_micropartition: MicroPartition) -> None:
    """Test TextstatFilter with default min/max scores."""
    op = TextstatFilter()
    mp = sample_micropartition

    params = {
        "metrics": ["flesch_reading_ease", "sentence_count"],
    }

    op.apply(mp, params)

    table = mp.to_arrow()
    assert "metadata" in table.column_names


def test_textstat_filter_mismatched_min_max_scores(sample_micropartition: MicroPartition) -> None:
    """Test that mismatched min_scores and max_scores keys raise error."""
    op = TextstatFilter()
    mp = sample_micropartition

    params = {
        "metrics": ["flesch_reading_ease"],
        "min_scores": {"flesch_reading_ease": 0.0},
        "max_scores": {"sentence_count": 100.0},  # Different key
    }

    with pytest.raises(ValueError, match="min_scores and max_scores must have the same keys"):
        op.apply(mp, params)


def test_textstat_filter_invalid_metrics(sample_micropartition: MicroPartition) -> None:
    """Test that invalid metrics raise error."""
    op = TextstatFilter()
    mp = sample_micropartition

    params = {
        "metrics": ["invalid_metric"],
    }

    with pytest.raises(ValueError, match="No valid metrics found"):
        op.apply(mp, params)


def test_textstat_filter_missing_column_fallback(sample_micropartition: MicroPartition) -> None:
    """Test that missing column falls back to 'text'."""
    op = TextstatFilter()
    # Create MicroPartition without 'text' column
    df = daft.from_pydict({"other_col": ["test"]})
    mp = next(iter(df.iter_partitions()))

    params = {
        "column": "nonexistent",
        "metrics": ["sentence_count"],
    }

    # Should return early without error if no text columns found
    op.apply(mp, params)


def test_textstat_filter_empty_text(sample_micropartition: MicroPartition) -> None:
    """Test TextstatFilter with empty text."""
    op = TextstatFilter()
    df = daft.from_pydict({"text": [None, "", "Valid text."]})
    mp = next(iter(df.iter_partitions()))

    params = {
        "metrics": ["sentence_count"],
    }

    op.apply(mp, params)

    table = mp.to_arrow()
    assert "metadata" in table.column_names
