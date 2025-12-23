"""Hooks framework for observability in FoundationDataFactory."""

from fdf.hooks.base import BaseHook
from fdf.hooks.whylogs_hook import WhyLogsHook

__all__ = ["BaseHook", "WhyLogsHook"]
