from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path
from typing import Any

import pyarrow as pa

from fdf.hooks.base import Hook


class WhylogsHook(Hook):
    """Optional whylogs hook for stage-level profiling."""

    def __init__(self, output_dir: str, columns: list[str] | None = None) -> None:
        self.output_dir = Path(output_dir)
        self.columns = columns
        self.enabled = False
        self.artifacts: list[str] = []
        try:
            self._whylogs = import_module("whylogs")
            self.enabled = True
        except ModuleNotFoundError:
            self._whylogs = None  # type: ignore[assignment]

        self._tables: list[pa.Table] = []
        self._stage_name: str | None = None

    def on_stage_start(self, stage_name: str) -> None:
        if not self.enabled:
            return
        self._stage_name = stage_name
        self._tables = []
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def on_partition_end(self, table: pa.Table) -> None:
        if not self.enabled:
            return
        if self.columns:
            cols = [col for col in self.columns if col in table.column_names]
            table = table.select(cols)
        self._tables.append(table)

    def on_stage_end(self, result: Any) -> None:
        if not self.enabled or not self._tables:
            return

        combined = pa.concat_tables(self._tables)
        df = combined.to_pandas()

        profile = None
        if hasattr(self._whylogs, "log"):
            profile = self._whylogs.log(df)
        elif hasattr(self._whylogs, "get_or_create_session"):
            session = self._whylogs.get_or_create_session()
            logger = session.logger()
            profile = logger.log_dataframe(df)

        stage_dir = self.output_dir / (self._stage_name or "stage")
        stage_dir.mkdir(parents=True, exist_ok=True)
        profile_path = stage_dir / "whylogs_profile.bin"

        if profile is not None:
            if hasattr(profile, "write"):
                profile.write(profile_path.as_posix())
            elif hasattr(profile, "writer"):
                profile.writer("local").write(profile_path.as_posix())
        else:
            # Fallback: write minimal JSON with row count to signal artifact creation.
            profile_path.write_text(json.dumps({"rows": combined.num_rows}))

        self.artifacts.append(profile_path.as_posix())
