"""Generic dataset writer.

Supports writing to various destinations using Daft connectors.
"""

from __future__ import annotations

import daft

from fdf.config.schema import DataSourceConfig, OutputConfig


def _write_file_based(df: daft.DataFrame, source: DataSourceConfig, writer: str) -> None:
    """Write to file-based data sources (parquet, json, csv)."""
    if source.path is None:
        msg = f"path is required for type='{source.type}'"
        raise ValueError(msg)
    if writer == "parquet":
        df.write_parquet(source.path)
    elif writer == "json":
        df.write_json(source.path)
    elif writer == "csv":
        df.write_csv(source.path)
    else:
        msg = f"Unknown writer: {writer}"
        raise ValueError(msg)


def write_data_source(df: daft.DataFrame, source: DataSourceConfig) -> None:
    """Write data to a data source configuration.

    Args:
        df: Daft DataFrame to write (should be materialized)
        source: Data source configuration
    """
    source_type = source.type

    if source_type in ("parquet", "json", "csv"):
        _write_file_based(df, source, source_type)
    elif source_type == "iceberg":
        if source.catalog is None or source.table is None:
            msg = "catalog and table are required for type='iceberg'"
            raise ValueError(msg)
        msg = "Iceberg writing not yet implemented"
        raise NotImplementedError(msg)
    elif source_type == "lance":
        if source.path is None:
            msg = "path is required for type='lance'"
            raise ValueError(msg)
        df.write_lance(source.path)
    elif source_type == "huggingface":
        if source.path is None:
            msg = "path is required for type='huggingface'"
            raise ValueError(msg)
        df.write_huggingface(source.path)
    else:
        msg = f"Unsupported data source type: {source_type}"
        raise ValueError(msg)


def write_output(df: daft.DataFrame, output_cfg: OutputConfig) -> None:
    """Write output dataset according to output configuration.

    Args:
        df: Daft DataFrame to write (should be materialized)
        output_cfg: Output configuration
    """
    write_data_source(df, output_cfg.source)
