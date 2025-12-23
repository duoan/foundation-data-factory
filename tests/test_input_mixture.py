from __future__ import annotations

import itertools

import datasets as hfds

from fdf.mixture.input_mixture import build_input_mixture


def test_mixture_respects_weights_approximately():
    ds_a = hfds.Dataset.from_dict({"v": ["a"] * 30})
    ds_b = hfds.Dataset.from_dict({"v": ["b"] * 10})

    mixture = build_input_mixture([ds_a, ds_b], weights=[0.75, 0.25], seed=42)

    first_40 = list(itertools.islice(mixture, 40))
    count_a = sum(1 for row in first_40 if row["v"] == "a")
    count_b = sum(1 for row in first_40 if row["v"] == "b")

    # Expect roughly 3:1 ratio
    assert count_a > count_b
    assert abs(count_a / count_b - 3) < 1.5


def test_mixture_supports_iterable_dataset_without_full_materialization():
    counter = {"seen": 0}

    def gen():
        for i in range(100):
            counter["seen"] += 1
            yield {"v": f"a{i}"}

    iterable = hfds.IterableDataset.from_generator(gen)
    ds_b = hfds.Dataset.from_dict({"v": ["b"] * 5})

    mixture = build_input_mixture([iterable, ds_b], weights=[0.8, 0.2], seed=0)

    first_5 = list(itertools.islice(mixture, 5))

    assert len(first_5) == 5
    # Ensure the generator was only advanced as needed (not fully consumed)
    assert counter["seen"] <= 10
