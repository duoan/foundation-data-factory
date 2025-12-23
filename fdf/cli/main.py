from __future__ import annotations

import argparse
from importlib.metadata import PackageNotFoundError, version

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

    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``fdf`` command-line interface."""

    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "version":
        print(_get_version())
        return 0

    # If no subcommand is provided, show help.
    parser.print_help()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
