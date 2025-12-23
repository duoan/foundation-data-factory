from __future__ import annotations

import textwrap

import datasets as hfds
import pyarrow as pa
import pytest

from fdf.config.schema import PipelineConfig
from fdf.runtime.local import run_stage_local


def _make_dataset() -> hfds.Dataset:
    return hfds.Dataset.from_dict({"x": [1, 2, 3]})


def _make_stage_with_passthrough() -> PipelineConfig:
    yaml_str = textwrap.dedent(
        """
        name: "local-runtime-test"
        input:
          type: "mixture"
        stages:
          - name: "stage-a"
            operators:
              - id: "refine.passthrough"
                kind: "refiner"
                op: "passthrough-refiner"
        output:
          type: "parquet"
        """
    ).strip()
    return PipelineConfig.from_yaml_str(yaml_str)


def test_run_stage_local_uses_daft_and_returns_hf_dataset(monkeypatch):
    dataset = _make_dataset()
    pipeline = _make_stage_with_passthrough()
    stage = pipeline.stages[0]

    # Monkeypatch Daft to observe that we actually call into it.
    calls: dict[str, pa.Table] = {}

    import fdf.runtime.local as local_rt

    def fake_from_arrow(tbl: pa.Table) -> str:  # type: ignore[override]
        calls["table"] = tbl
        return "DAFT_DF"

    monkeypatch.setattr(local_rt.daft, "from_arrow", fake_from_arrow, raising=True)

    result = run_stage_local(dataset, stage)

    assert isinstance(result, hfds.Dataset)
    assert list(result["x"]) == [1, 2, 3]
    # Ensure we went through the Daft conversion path at least once.
    assert "table" in calls


def test_run_stage_local_rejects_iterable_dataset(tmp_path):
    pipeline = _make_stage_with_passthrough()
    stage = pipeline.stages[0]

    iterable = hfds.IterableDataset.from_generator(lambda: ({"x": i} for i in range(3)))

    with pytest.raises(TypeError, match=r"only supports datasets\.Dataset"):
        run_stage_local(iterable, stage)
