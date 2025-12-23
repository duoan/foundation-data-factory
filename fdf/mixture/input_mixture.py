from __future__ import annotations

import random
from collections.abc import Iterable, Iterator

import datasets as hfds


def _to_iterable(ds: hfds.Dataset | hfds.IterableDataset) -> hfds.IterableDataset:
    """Convert a Dataset to IterableDataset without full materialization."""

    if isinstance(ds, hfds.IterableDataset):
        return ds
    # HF provides a native conversion that keeps streaming semantics.
    return ds.to_iterable_dataset()


class InvalidMixtureWeightsError(ValueError):
    """Raised when mixture weights are invalid."""

    def __init__(self) -> None:
        super().__init__("Mixture weights must sum to a positive value.")


class DatasetWeightsLengthMismatchError(ValueError):
    """Raised when datasets and weights lengths differ."""

    def __init__(self) -> None:
        super().__init__("datasets and weights must have the same length.")


def _normalize_weights(weights: Iterable[float]) -> list[float]:
    weights = list(weights)
    total = sum(weights)
    if total <= 0:
        raise InvalidMixtureWeightsError()
    return [w / total for w in weights]


def _interleave_iterators(
    iters: list[Iterator[dict]],
    probs: list[float],
    rng: random.Random,
) -> Iterator[dict]:
    """Yield rows from iterators according to probability weights."""

    active: list[tuple[Iterator[dict], float]] = list(zip(iters, probs))

    while active:
        # Choose an iterator index proportionally to its probability among actives.
        _, weights = zip(*active)
        total = sum(weights)
        norm_weights = [w / total for w in weights]
        idx = rng.choices(range(len(active)), weights=norm_weights, k=1)[0]
        it, _ = active[idx]
        try:
            yield next(it)
        except StopIteration:
            active.pop(idx)


def _mixture_generator(datasets: list[hfds.Dataset | hfds.IterableDataset], weights: list[float], seed: int | None):
    rng = random.Random(seed)  # noqa: S311 - non-crypto RNG is intended for sampling
    probs = _normalize_weights(weights)
    iters = [_to_iterable(ds).__iter__() for ds in datasets]
    yield from _interleave_iterators(iters, probs, rng)


def build_input_mixture(
    datasets: list[hfds.Dataset | hfds.IterableDataset],
    *,
    weights: list[float],
    seed: int | None = None,
) -> hfds.IterableDataset:
    """Build a weighted input mixture as an IterableDataset.

    - Supports HF Dataset and IterableDataset without global shuffle.
    - Streaming-friendly: consumes source iterators on demand.
    - Approximates the given weights via probabilistic interleave.
    """

    if len(datasets) != len(weights):
        raise DatasetWeightsLengthMismatchError()

    return hfds.IterableDataset.from_generator(
        _mixture_generator,
        gen_kwargs={"datasets": datasets, "weights": weights, "seed": seed},
    )
