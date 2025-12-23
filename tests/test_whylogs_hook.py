from __future__ import annotations

import json
import types
from pathlib import Path

import datasets as hfds

from fdf.config.schema import PipelineConfig
from fdf.hooks.whylogs_hook import WhylogsHook
from fdf.runtime.local import run_stage_local


def _stage_with_materialize(tmp_path: Path):
    cfg = PipelineConfig.from_yaml_str(
        f"""
        name: "whylogs-test"
        input:
          type: "mixture"
        stages:
          - name: "stage-a"
            operators:
              - id: "refine.passthrough"
                kind: "refiner"
                op: "passthrough-refiner"
            materialize:
              path: "{tmp_path.as_posix()}"
              mode: "overwrite"
        output:
          type: "parquet"
        """
    )
    return cfg.stages[0]


def test_whylogs_hook_no_dependency_is_noop(monkeypatch, tmp_path):
    monkeypatch.setattr("fdf.hooks.whylogs_hook.import_module", lambda name: (_ for _ in ()).throw(ModuleNotFoundError))

    hook = WhylogsHook(output_dir=tmp_path.as_posix(), columns=["x"])
    stage = _stage_with_materialize(tmp_path)
    ds = hfds.Dataset.from_dict({"x": [1, 2, 3]})

    result = run_stage_local(ds, stage, hooks=[hook])

    assert list(result["x"]) == [1, 2, 3]
    assert not hook.enabled
    assert not hook.artifacts
    assert not any(tmp_path.glob("**/*profile*"))


def test_whylogs_hook_writes_artifact_and_manifest(monkeypatch, tmp_path):
    class FakeProfile:
        def write(self, path: str):
            Path(path).write_text("profile")

    def fake_log(df):
        return FakeProfile()

    fake_module = types.SimpleNamespace(log=fake_log)
    monkeypatch.setattr("fdf.hooks.whylogs_hook.import_module", lambda name: fake_module)

    hook = WhylogsHook(output_dir=tmp_path.as_posix(), columns=["x"])
    stage = _stage_with_materialize(tmp_path)
    ds = hfds.Dataset.from_dict({"x": [5, 6]})

    result = run_stage_local(ds, stage, hooks=[hook])

    assert list(result["x"]) == [5, 6]
    assert hook.enabled
    assert hook.artifacts
    profile_path = Path(hook.artifacts[0])
    assert profile_path.exists()

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert profile_path.as_posix() in manifest.get("hook_artifacts", [])
