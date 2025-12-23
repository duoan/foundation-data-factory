from __future__ import annotations


class BaseHook:
    """Base class for runtime hooks.

    Hooks are optional and must not affect execution semantics.
    """

    def on_stage_start(self, *, stage_name: str) -> None:  # pragma: no cover - default no-op
        return None

    def on_partition_end(self, *, stage_name: str, rows: int) -> None:  # pragma: no cover - default no-op
        return None

    def on_stage_end(self, *, stage_name: str, output_rows: int) -> None:  # pragma: no cover - default no-op
        return None
