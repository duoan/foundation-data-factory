"""Operator base classes and built-in implementations."""

# Import optional operators (they handle missing dependencies internally)
from . import textstat_filter  # noqa: F401
from .base import BatchOperator
from .passthrough import PassthroughRefiner
from .registry import get_operator_class, register_operator

__all__ = [
    "BatchOperator",
    "PassthroughRefiner",
    "get_operator_class",
    "register_operator",
]
