from __future__ import annotations

import pyarrow as pa

from fdf.operators.passthrough import PassthroughRefiner


def test_passthrough_refiner_metadata():
    op = PassthroughRefiner()

    assert op.name == "passthrough-refiner"
    assert op.kind == "refiner"
    assert isinstance(op.version, str)


def test_passthrough_refiner_returns_same_batch():
    table = pa.table({"x": [1, 2, 3]})
    op = PassthroughRefiner()

    result = op.apply(table)

    # Passthrough should not modify the batch.
    assert result is table
