from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import datasets as hfds

from fdf.config.schema import PipelineConfig
from fdf.hooks.whylogs_hook import WhyLogsHook
from fdf.runtime.local import run_stage_local


def _make_stage(materialize_path: Path):
    yaml_str = f"""
    name: test-whylogs
    input:
      type: mixture
    stages:
      - name: stage-a
        operators:
          - id: refine.passthrough
            kind: refiner
            op: passthrough-refiner
        materialize:
          path: "{materialize_path.as_posix()}"
          mode: overwrite
    output:
      type: parquet
    """
    return PipelineConfig.from_yaml_str(yaml_str).stages[0]


def test_whylogs_hook_disabled_when_missing(tmp_path, monkeypatch):
    # Ensure whylogs is not importable
    monkeypatch.setitem(sys.modules, "whylogs", None)
    stage = _make_stage(tmp_path)
    dataset = hfds.Dataset.from_dict({"x": [1, 2]})

    hook = WhyLogsHook(output_dir=tmp_path / "profiles")

    run_stage_local(dataset, stage, hooks=[hook])

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert "artifacts" not in manifest
    assert not (tmp_path / "profiles").exists() or not list((tmp_path / "profiles").glob("*.json"))


def test_whylogs_hook_writes_artifact_and_manifest_entry(tmp_path, monkeypatch):
    # Provide a dummy whylogs module to enable the hook without installing the package.
    monkeypatch.setitem(sys.modules, "whylogs", SimpleNamespace())

    stage = _make_stage(tmp_path)
    dataset = hfds.Dataset.from_dict({"x": [1, 2, 3]})

    profile_dir = tmp_path / "profiles"
    hook = WhyLogsHook(output_dir=profile_dir)

    run_stage_local(dataset, stage, hooks=[hook])

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    artifacts = manifest.get("artifacts", [])
    assert len(artifacts) == 1
    assert "whylogs" in artifacts[0]
    artifact_path = Path(artifacts[0]["whylogs"][0])
    assert artifact_path.exists()

    payload = json.loads(artifact_path.read_text())
    assert payload["stage"] == "stage-a"
    assert payload["rows"] == 3
