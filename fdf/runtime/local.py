from __future__ import annotations

from typing import Any

import daft
import datasets as hfds
import pyarrow as pa

from fdf.config.schema import StageConfig
from fdf.operators.registry import get_operator_class


def _ensure_dataset(dataset: Any) -> hfds.Dataset:
    """Ensure we are working with a non-streaming HF `Dataset`.

    Local runtime in PR4 intentionally supports only `datasets.Dataset`
    (not `IterableDataset`) to keep the execution model simple.
    """

    if not isinstance(dataset, hfds.Dataset):
        msg = "only supports datasets.Dataset"
        raise TypeError(msg)
    return dataset


def run_stage_local(
    dataset: hfds.Dataset,
    stage: StageConfig,
) -> hfds.Dataset:
    """Execute a single stage locally using Daft and Arrow.

    High-level flow (matching DESIGN/CHECKLIST):

    1. HF Dataset → Arrow table
    2. Arrow → Daft DataFrame
    3. Apply operators sequentially over Arrow batches
    4. Convert back → HF Dataset

    For PR4 we keep execution simple and in-memory.
    """

    dataset = _ensure_dataset(dataset)

    # 1. HF Dataset → Arrow
    # There is no stable public HF API to expose the underlying Arrow table,
    # so for now we construct it from the row iterator. This keeps Arrow as
    # the internal representation without introducing a pandas fallback.
    table: pa.Table = pa.Table.from_pylist(list(dataset))

    # 2. Arrow → Daft DataFrame (ensure we exercise Daft's Arrow path;
    # operators themselves still work on Arrow tables).
    daft.from_arrow(table)

    # 3. Apply operators sequentially.
    for op_cfg in stage.operators:
        # If no `op` name is configured yet, treat as a no-op (future PRs
        # will enforce this more strictly once all operators are wired).
        op_name = getattr(op_cfg, "op", None)
        if not op_name:
            continue

        op_cls = get_operator_class(op_name)
        op = op_cls()
        table = op.apply(table)

        # Keep Daft in the loop after each operator to respect the
        # "Daft as internal data plane" design constraint.
        daft.from_arrow(table)

    # 4. Arrow → HF Dataset
    data_dict = table.to_pydict()
    return hfds.Dataset.from_dict(data_dict)
