"""Generic dataset reader.

Supports reading from various sources using Daft connectors.
"""

from __future__ import annotations

import json
from pathlib import Path

import daft

from fdf.config.schema import DataSourceConfig, InputConfig


def _read_parquet(source: DataSourceConfig) -> daft.DataFrame:
    """Read parquet data source."""
    if source.path is None:
        msg = "path is required for type='parquet'"
        raise ValueError(msg)
    path_obj = Path(source.path)
    if path_obj.is_dir():
        parquet_files = [str(p) for p in path_obj.glob("*.parquet")]
        if not parquet_files:
            msg = f"No parquet files found in {source.path}"
            raise ValueError(msg)
        return daft.read_parquet(parquet_files)
    return daft.read_parquet(source.path)


def _read_huggingface(source: DataSourceConfig) -> daft.DataFrame:
    """Read Hugging Face dataset."""
    if source.path is None:
        msg = "path is required for type='huggingface'"
        raise ValueError(msg)
    import datasets as hfds

    dataset_path = source.path
    if source.streaming:
        hfds.load_dataset(dataset_path, streaming=True, split="train", token=source.token)
        msg = "Streaming Hugging Face datasets not yet fully supported. Use streaming=false for now."
        raise NotImplementedError(msg)
    dataset = hfds.load_dataset(dataset_path, split="train", token=source.token)
    return daft.from_arrow(dataset.data.table)


def _read_file_based(source: DataSourceConfig, reader: str) -> daft.DataFrame:
    """Read file-based data sources (json, csv)."""
    if source.path is None:
        msg = f"path is required for type='{source.type}'"
        raise ValueError(msg)
    if reader == "json":
        return daft.read_json(source.path)
    if reader == "csv":
        return daft.read_csv(source.path)
    msg = f"Unknown reader: {reader}"
    raise ValueError(msg)


def read_data_source(source: DataSourceConfig) -> daft.DataFrame:
    """Read data from a data source configuration as Daft DataFrame.

    Args:
        source: Data source configuration

    Returns:
        Daft DataFrame (lazy, not materialized)
    """
    source_type = source.type

    if source_type == "parquet":
        return _read_parquet(source)
    if source_type in ("json", "csv"):
        return _read_file_based(source, source_type)
    if source_type == "iceberg":
        if source.catalog is None or source.table is None:
            msg = "catalog and table are required for type='iceberg'"
            raise ValueError(msg)
        msg = "Iceberg reading not yet implemented"
        raise NotImplementedError(msg)
    if source_type == "lance":
        if source.path is None:
            msg = "path is required for type='lance'"
            raise ValueError(msg)
        return daft.read_lance(source.path)
    if source_type == "huggingface":
        return _read_huggingface(source)

    msg = f"Unsupported data source type: {source_type}"
    raise ValueError(msg)


def read_input(input_cfg: InputConfig) -> daft.DataFrame:
    """Read input dataset from input configuration as Daft DataFrame.

    Args:
        input_cfg: Input configuration

    Returns:
        Daft DataFrame (lazy, not materialized)
    """
    # Handle mixture input
    if input_cfg.mixture is not None:
        # For mixture, we need to load each dataset and combine
        # TODO: Implement mixture support using Daft
        msg = "Mixture input not yet implemented with Daft. Use single data source."
        raise NotImplementedError(msg)

    # Handle single source input
    if input_cfg.source is not None:
        return read_data_source(input_cfg.source)

    # Backward compatibility: handle old format where type and path are at top level
    if input_cfg.type is not None:
        # If type is "mixture", it should have been handled above
        if input_cfg.type == "mixture":
            msg = "Mixture input not yet implemented with Daft. Use single data source."
            raise NotImplementedError(msg)
        # Create a temporary DataSourceConfig for backward compatibility
        source = DataSourceConfig(type=input_cfg.type, path=getattr(input_cfg, "path", None))
        return read_data_source(source)

    msg = "Input configuration must specify either source or mixture"
    raise ValueError(msg)


def read_from_materialize_path(materialize_path: str) -> daft.DataFrame:
    """Read dataset from a stage's materialize path as Daft DataFrame.

    Args:
        materialize_path: Path to the materialize directory

    Returns:
        Daft DataFrame (lazy, not materialized)
    """
    base = Path(materialize_path)
    manifest_path = base / "manifest.json"

    if not manifest_path.exists():
        msg = f"Manifest not found at {manifest_path}"
        raise FileNotFoundError(msg)

    manifest = json.loads(manifest_path.read_text())
    shards = manifest.get("shards", [])

    if not shards:
        msg = f"No shards found in manifest at {manifest_path}"
        raise ValueError(msg)

    # Read all shards using Daft
    # Daft can read multiple parquet files
    return daft.read_parquet(shards)
