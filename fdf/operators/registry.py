from __future__ import annotations

from typing import TypeVar

from .base import BatchOperator

_T = TypeVar("_T", bound=type[BatchOperator])


class OperatorRegistrationError(RuntimeError):
    """Raised when operator registration fails (e.g., duplicate names)."""


class UnknownOperatorError(KeyError):
    """Raised when an operator name is not present in the registry."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Operator {name!r} is not registered")


_REGISTRY: dict[str, type[BatchOperator]] = {}


def register_operator(cls: _T) -> _T:
    """Class decorator to register an operator implementation.

    Usage:

    .. code-block:: python

        @register_operator
        class MyOperator(BatchOperator):
            name = "my-operator"
            version = "1.0.0"
            kind = "refiner"
    """

    name = getattr(cls, "name", None)
    if not name:
        msg = "Operator classes must define a non-empty 'name' attribute for registration."
        raise OperatorRegistrationError(msg)

    if name in _REGISTRY:
        msg = f"Operator with name {name!r} is already registered."
        raise OperatorRegistrationError(msg)

    _REGISTRY[name] = cls
    return cls


def get_operator_class(name: str) -> type[BatchOperator]:
    """Return the registered operator class for the given name."""

    try:
        return _REGISTRY[name]
    except KeyError as exc:  # pragma: no cover - simple KeyError path
        raise UnknownOperatorError(name) from exc
