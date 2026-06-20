"""Stdlib-only Parquet stats needed to register a file with DuckLake.

`register_data_file()` needs the file size and the Parquet footer size (the
length of the file metadata, used by readers to locate it). Both can be read
without a Parquet library: the last 8 bytes of every Parquet file are a 4-byte
little-endian footer length followed by the ASCII magic ``PAR1``.

Row count is intentionally NOT computed here — that needs a Parquet reader
(duckdb/pyarrow), which the caller already has, and keeping this module
dependency-free preserves the package's single-dependency (SQLAlchemy) footprint.
"""
from __future__ import annotations

import os
import struct

__all__ = ["footer_and_size"]


def footer_and_size(path: str) -> tuple[int, int]:
    """Return ``(file_size_bytes, footer_size)`` for a Parquet file on local disk.

    Raises ValueError if the file is not a valid Parquet file (missing PAR1 magic).
    """
    file_size_bytes = os.path.getsize(path)
    with open(path, "rb") as f:
        f.seek(-8, os.SEEK_END)
        footer_size = struct.unpack("<i", f.read(4))[0]
        if f.read(4) != b"PAR1":
            raise ValueError(f"{path}: not a Parquet file (missing PAR1 magic)")
    return file_size_bytes, footer_size
