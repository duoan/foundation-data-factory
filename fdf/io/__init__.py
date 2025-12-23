"""IO layer for reading and writing datasets.

This module provides generic readers and writers that abstract over different
data sources. Currently supports Hugging Face datasets via Daft.
"""

from fdf.io.reader import read_input
from fdf.io.writer import write_output

__all__ = ["read_input", "write_output"]
