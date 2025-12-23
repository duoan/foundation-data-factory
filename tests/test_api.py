from __future__ import annotations

import textwrap

import datasets as hfds
import pytest

from fdf.api import run_pipeline


def _write_minimal_pipeline(tmp_path) -> str:
    path = tmp_path / "pipeline.yaml"
    path.write_text(
        textwrap.dedent(
            """
            name: "api-test"
            input:
              type: "mixture"
            stages:
              - name: "stage-a"
                operators: []
            output:
              type: "parquet"
            """
        ).strip()
    )
    return str(path)


def test_run_pipeline_accepts_hf_dataset_and_returns_dataset(tmp_path):
    pipeline_path = _write_minimal_pipeline(tmp_path)
    dataset = hfds.Dataset.from_dict({"text": ["a", "b"]})

    result = run_pipeline(dataset, pipeline_path)

    assert result is dataset
    assert isinstance(result, hfds.Dataset)


def test_run_pipeline_accepts_iterable_dataset(tmp_path):
    pipeline_path = _write_minimal_pipeline(tmp_path)
    iterable = hfds.IterableDataset.from_generator(lambda: ({"x": i} for i in range(3)))

    result = run_pipeline(iterable, pipeline_path)

    assert result is iterable
    assert isinstance(result, hfds.IterableDataset)


@pytest.mark.parametrize("bad_input", [123, "not-a-dataset", {"a": 1}, [1, 2, 3]])
def test_run_pipeline_rejects_non_hf_dataset(bad_input, tmp_path):
    pipeline_path = _write_minimal_pipeline(tmp_path)

    with pytest.raises(TypeError, match="Hugging Face Dataset"):
        run_pipeline(bad_input, pipeline_path)
