from __future__ import annotations

import argparse
import sys
from importlib.metadata import PackageNotFoundError, version

from fdf.api import run_pipeline_from_yaml
from fdf.config.schema import PipelineConfig

PROJECT_NAME = "foundation-data-factory"


def _get_version() -> str:
    """Return the installed package version.

    Falls back to ``unknown`` if the package metadata is not available.
    """

    try:
        return version(PROJECT_NAME)
    except PackageNotFoundError:  # pragma: no cover - defensive fallback
        return "unknown"


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI argument parser."""

    parser = argparse.ArgumentParser(
        prog="fdf",
        description="FoundationDataFactory (FDF) — streaming-first data execution system for foundation models.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # `fdf version`
    subparsers.add_parser("version", help="Show the installed FoundationDataFactory version.")

    # `fdf validate pipeline.yaml`
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate a pipeline YAML configuration file.",
    )
    validate_parser.add_argument(
        "pipeline_yaml",
        help="Path to the pipeline YAML configuration file.",
    )

    # `fdf run pipeline.yaml`
    run_parser = subparsers.add_parser(
        "run",
        help="Run a pipeline from YAML configuration file.",
    )
    run_parser.add_argument(
        "pipeline_yaml",
        help="Path to the pipeline YAML configuration file.",
    )
    run_parser.add_argument(
        "--ray-address",
        help="Ray cluster address (e.g., ray://127.0.0.1:10001). If not provided, runs locally.",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``fdf`` command-line interface."""

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "version":
        print(_get_version())
        return 0

    if args.command == "validate":
        try:
            PipelineConfig.from_yaml_file(args.pipeline_yaml)
            print(f"✓ Pipeline configuration is valid: {args.pipeline_yaml}")
        except Exception as exc:
            print(f"Validation failed: {exc}", file=sys.stderr)
            return 1

        return 0

    if args.command == "run":
        try:
            result = run_pipeline_from_yaml(args.pipeline_yaml, ray_address=getattr(args, "ray_address", None))
            print(f"✓ Pipeline completed successfully. Output dataset has {len(result)} rows.")
        except Exception as exc:
            print(f"Pipeline execution failed: {exc}", file=sys.stderr)
            return 1

        return 0

    # If no subcommand is provided, show help.
    parser.print_help()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
