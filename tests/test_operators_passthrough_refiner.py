from __future__ import annotations

import pyarrow as pa
from daft.recordbatch.micropartition import MicroPartition

from fdf.operators.passthrough import PassthroughRefiner


def test_passthrough_refiner_metadata():
    op = PassthroughRefiner()

    assert op.name == "passthrough-refiner"
    assert op.kind == "refiner"
    assert isinstance(op.version, str)


def test_passthrough_refiner_modifies_in_place():
    """Test that passthrough refiner applies in-place (no-op for passthrough)."""
    table = pa.table({"x": [1, 2, 3]})
    partition = MicroPartition.from_arrow(table)
    op = PassthroughRefiner()

    # Apply operator in-place (should not modify for passthrough)
    op.apply(partition)

    # Verify partition is unchanged (passthrough is no-op)
    result_table = partition.to_arrow()
    assert result_table.num_rows == 3
    # Convert PyArrow array to Python list for comparison
    assert result_table["x"].to_pylist() == [1, 2, 3]
