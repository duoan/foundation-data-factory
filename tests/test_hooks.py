from __future__ import annotations

import datasets as hfds

from fdf.config.schema import PipelineConfig
from fdf.hooks.base import BaseHook
from fdf.runtime.local import run_stage_local


class RecordingHook(BaseHook):
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def on_stage_start(self, *, stage_name: str) -> None:
        self.events.append(("start", {"stage": stage_name}))

    def on_partition_end(self, *, stage_name: str, rows: int) -> None:
        self.events.append(("partition_end", {"stage": stage_name, "rows": rows}))

    def on_stage_end(self, *, stage_name: str, output_rows: int) -> None:
        self.events.append(("end", {"stage": stage_name, "rows": output_rows}))


def _make_stage():
    yaml_str = """
    name: test-hooks
    input:
      type: mixture
    stages:
      - name: stage-a
        operators:
          - id: refine.passthrough
            kind: refiner
            op: passthrough-refiner
    output:
      type: parquet
    """
    return PipelineConfig.from_yaml_str(yaml_str).stages[0]


def test_hooks_are_called_in_order():
    stage = _make_stage()
    dataset = hfds.Dataset.from_dict({"x": [1, 2]})
    hook = RecordingHook()

    run_stage_local(dataset, stage, hooks=[hook])

    kinds = [k for k, _ in hook.events]
    assert kinds == ["start", "partition_end", "end"]

    assert hook.events[0][1]["stage"] == "stage-a"
    assert hook.events[1][1]["rows"] == 2
    assert hook.events[2][1]["rows"] == 2
