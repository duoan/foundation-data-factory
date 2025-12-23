from __future__ import annotations

import datasets as hfds

from fdf.config.schema import PipelineConfig
from fdf.hooks.base import Hook
from fdf.runtime.local import run_stage_local


class RecordingHook(Hook):
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def on_stage_start(self, stage_name: str) -> None:
        self.events.append(("start", {"stage": stage_name}))

    def on_partition_end(self, table) -> None:
        self.events.append(("partition_end", {"rows": table.num_rows}))

    def on_stage_end(self, result: hfds.Dataset) -> None:
        self.events.append(("end", {"rows": len(result)}))


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
