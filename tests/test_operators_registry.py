from __future__ import annotations

import pytest

from fdf.operators.base import BatchOperator
from fdf.operators.registry import (
    OperatorRegistrationError,
    UnknownOperatorError,
    get_operator_class,
    register_operator,
)


class DummyBatchOperator(BatchOperator):
    """Minimal operator implementation used only for registry tests."""

    name = "dummy"
    version = "0.0.test"
    kind = "refiner"

    def apply(
        self, batch, params=None
    ) -> None:  # pragma: no cover - unreachable in registry tests  # type: ignore[override]
        """In-place operator (no-op for dummy)."""
        pass


def test_register_and_lookup_operator_class():
    @register_operator
    class MyOp(DummyBatchOperator):  # type: ignore[abstract]
        name = "my-op"
        version = "1.0.0"

    cls = get_operator_class("my-op")

    assert cls is MyOp
    assert issubclass(cls, BatchOperator)
    assert cls.kind == "refiner"


def test_duplicate_registration_fails():
    @register_operator
    class FirstOp(DummyBatchOperator):  # type: ignore[abstract]
        name = "dup-op"
        version = "1.0.0"

    assert get_operator_class("dup-op") is FirstOp

    with pytest.raises(OperatorRegistrationError, match="already registered"):

        @register_operator
        class SecondOp(DummyBatchOperator):  # type: ignore[abstract]
            name = "dup-op"
            version = "1.0.1"


def test_get_operator_class_unknown_name_raises_unknown_operator_error():
    with pytest.raises(UnknownOperatorError):
        get_operator_class("non-existent-operator")
