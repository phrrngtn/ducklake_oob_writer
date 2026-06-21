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

__all__ = ["footer_and_size", "column_stats"]


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


def column_stats(path: str):
    """Compute the per-column statistics DuckLake needs to make a file
    compaction-ready: ``(record_count, [(column_name, stats), ...])`` in schema
    order, where ``stats`` has ``value_count``, ``null_count``,
    ``column_size_bytes``, ``min_value``, ``max_value``, ``contains_nan``.

    Reads the Parquet file with DuckDB (lazy import), so this is **not** part of
    the dependency-free core — call it from a context that already has duckdb
    (and pass the result to ``DuckLakeWriter.register_data_file(column_stats=...)``,
    or just use ``DuckLakeWriter.register_parquet`` which calls this for you).

    min/max are stored as DuckDB's ``CAST(... AS VARCHAR)`` form, which is the
    same encoding DuckLake itself uses.
    """
    import duckdb  # lazy

    con = duckdb.connect()
    try:
        schema = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [path]).fetchall()
        total = con.execute("SELECT count(*) FROM read_parquet(?)", [path]).fetchone()[0]
        sizes = {
            name: (int(sz) if sz is not None else None)
            for name, sz in con.execute(
                "SELECT path_in_schema, sum(total_compressed_size) "
                "FROM parquet_metadata(?) GROUP BY path_in_schema", [path]).fetchall()
        }
        out = []
        for col in schema:
            name, typ = col[0], (col[1] or "").upper()
            q = '"' + name.replace('"', '""') + '"'
            vc, mn, mx = con.execute(
                f"SELECT count({q}), CAST(min({q}) AS VARCHAR), CAST(max({q}) AS VARCHAR) "
                "FROM read_parquet(?)", [path]).fetchone()
            contains_nan = None
            if any(t in typ for t in ("FLOAT", "DOUBLE", "REAL")):
                nan = con.execute(
                    f"SELECT count(*) FILTER (WHERE isnan({q})) FROM read_parquet(?)", [path]
                ).fetchone()[0]
                contains_nan = 1 if (nan or 0) > 0 else 0
            out.append((name, {
                "value_count": int(vc),
                "null_count": int(total) - int(vc),
                "column_size_bytes": sizes.get(name),
                "min_value": mn,
                "max_value": mx,
                "contains_nan": contains_nan,
            }))
        return int(total), out
    finally:
        con.close()
