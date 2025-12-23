from __future__ import annotations

import json
from pathlib import Path

from fdf.hooks.base import BaseHook


class WhyLogsHook(BaseHook):
    """Optional whylogs profiling hook.

    - If whylogs is not installed, the hook is disabled silently.
    - When enabled, writes a simple artifact per stage with row counts.
    - Exposes artifact path via `manifest_artifacts`.
    """

    def __init__(self, output_dir: str) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        try:
            import whylogs  # noqa: F401
        except ModuleNotFoundError:
            self.enabled = False
        else:
            self.enabled = True

        self._artifact_path: Path | None = None

    def on_stage_end(self, *, stage_name: str, output_rows: int) -> None:
        if not self.enabled:
            return

        artifact = {
            "stage": stage_name,
            "rows": output_rows,
        }
        self._artifact_path = self.output_dir / f"{stage_name}_whylogs.json"
        self._artifact_path.write_text(json.dumps(artifact, indent=2))

    def manifest_artifacts(self) -> dict | None:
        if self._artifact_path and self._artifact_path.exists():
            return {"whylogs": [self._artifact_path.as_posix()]}
        return None
