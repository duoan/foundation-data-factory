from __future__ import annotations

import textwrap

from fdf.cli.main import main as fdf_main


def _write_yaml(tmp_path, content: str) -> str:
    path = tmp_path / "pipeline.yaml"
    path.write_text(textwrap.dedent(content).strip())
    return str(path)


def test_cli_validate_succeeds_for_valid_pipeline(tmp_path):
    yaml_content = """
        name: "cli-valid"

        input:
          type: "mixture"

        stages:
          - name: "stage-a"
            operators:
              - id: "op-a"
                kind: "score"

        output:
          type: "parquet"
    """
    pipeline_path = _write_yaml(tmp_path, yaml_content)

    exit_code = fdf_main(["validate", pipeline_path])

    assert exit_code == 0


def test_cli_validate_fails_for_invalid_pipeline(tmp_path, capsys):
    # Duplicate stage names should be rejected by the schema layer.
    yaml_content = """
        name: "cli-invalid"

        input:
          type: "mixture"

        stages:
          - name: "dup-stage"
            operators: []
          - name: "dup-stage"
            operators: []

        output:
          type: "parquet"
    """
    pipeline_path = _write_yaml(tmp_path, yaml_content)

    exit_code = fdf_main(["validate", pipeline_path])
    captured = capsys.readouterr()

    assert exit_code != 0
    # Basic sanity check that we printed some message about validation failure.
    assert "validation failed" in captured.err.lower()
