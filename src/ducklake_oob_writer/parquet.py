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

from sqlalchemy import create_engine, text

__all__ = ["footer_and_size", "column_stats", "content_hash", "write_rows_parquet"]


def duck_engine():
    """A transient in-memory SQLAlchemy duckdb engine for the file *reads* (read_parquet /
    file_row_number). Needs ``duckdb-engine``. Writing a Parquet doesn't involve a database
    — see ``write_rows_parquet``."""
    return create_engine("duckdb:///:memory:")


def _arrow_type(pa, ducklake_type):
    base = ducklake_type.split("(", 1)[0].strip().lower()
    if base in ("decimal", "numeric"):
        import re
        m = re.search(r"\((\d+)\s*,\s*(\d+)\)", ducklake_type)
        return pa.decimal128(int(m.group(1)), int(m.group(2))) if m else pa.decimal128(38, 9)
    return {
        "int8": pa.int8(), "int16": pa.int16(), "int32": pa.int32(), "int64": pa.int64(),
        "integer": pa.int64(), "bigint": pa.int64(), "smallint": pa.int16(), "tinyint": pa.int8(),
        "float32": pa.float32(), "float64": pa.float64(), "float": pa.float64(), "double": pa.float64(),
        "varchar": pa.string(), "string": pa.string(), "text": pa.string(),
        "boolean": pa.bool_(), "bool": pa.bool_(),
        "date": pa.date32(), "timestamp": pa.timestamp("us"), "time": pa.time64("us"),
    }.get(base, pa.string())


def write_rows_parquet(colspecs, rows, path):
    """Write ``rows`` to a Parquet file **directly with pyarrow** — no database. Writing a
    file from rows you already hold isn't a DB operation (reach for duckdb only when you
    need a join / scan / the replacement scan). The explicit Arrow schema makes the Parquet
    column types match the DuckLake table exactly. ``colspecs`` = ``[(name, ducklake_type),
    …]``; ``rows`` = tuples in that column order. Needs ``pyarrow``."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    names = [n for n, _ in colspecs]
    schema = pa.schema([(n, _arrow_type(pa, t)) for n, t in colspecs])
    table = pa.table({n: [r[i] for r in rows] for i, n in enumerate(names)}, schema=schema)
    pq.write_table(table, path)


def _is_remote(path: str) -> bool:
    """True for object-store / remote URIs (s3://, gs://, …), False for local/file://."""
    i = path.find("://")
    return i != -1 and path[:i] != "file"


def content_hash(path: str, *, algo: str = "sha256", storage_options: dict | None = None) -> str:
    """Hex content digest of a file (default sha256), read in chunks — the content
    address / dedup key for the incorporation log. Local paths use the stdlib;
    object-store URIs need fsspec (the ``[s3]`` extra) and ``storage_options``."""
    import hashlib

    h = hashlib.new(algo)
    if _is_remote(path):
        import fsspec

        with fsspec.open(path, "rb", **(storage_options or {})) as f:
            for block in iter(lambda: f.read(1 << 20), b""):
                h.update(block)
    else:
        with open(path, "rb") as f:
            for block in iter(lambda: f.read(1 << 20), b""):
                h.update(block)
    return h.hexdigest()


def _footer_from_tail(path: str, tail: bytes, size: int) -> tuple[int, int]:
    footer_size = struct.unpack("<i", tail[:4])[0]
    if tail[4:] != b"PAR1":
        raise ValueError(f"{path}: not a Parquet file (missing PAR1 magic)")
    return int(size), footer_size


def footer_and_size(path: str, storage_options: dict | None = None) -> tuple[int, int]:
    """Return ``(file_size_bytes, footer_size)`` for a Parquet file.

    Works for local paths (stdlib) and object-store URIs like ``s3://…`` (via
    ``fsspec``/``s3fs`` — the optional ``[s3]`` extra). ``storage_options`` is
    passed to fsspec for remote paths (e.g. MinIO: ``{"key": …, "secret": …,
    "client_kwargs": {"endpoint_url": "http://host:9000"}}``).

    Raises ValueError if the file is not a valid Parquet file (missing PAR1 magic).
    """
    if _is_remote(path):
        import fsspec  # lazy: only needed for remote paths (optional [s3] extra)

        fs, _, paths = fsspec.get_fs_token_paths(path, storage_options=storage_options or {})
        rp = paths[0]
        size = fs.size(rp)
        with fs.open(rp, "rb") as f:
            f.seek(size - 8)
            tail = f.read(8)
        return _footer_from_tail(path, tail, size)

    size = os.path.getsize(path)
    with open(path, "rb") as f:
        f.seek(-8, os.SEEK_END)
        tail = f.read(8)
    return _footer_from_tail(path, tail, size)


def column_stats(path: str, con=None):
    """Compute the per-column statistics for query pruning:
    ``(record_count, [(column_name, stats), ...])`` in schema order, where
    ``stats`` has ``value_count``, ``null_count``, ``column_size_bytes``,
    ``min_value``, ``max_value``, ``contains_nan``.

    Reads the Parquet file with DuckDB (lazy import), so this is **not** part of
    the dependency-free core. For object-store paths (``s3://…``) pass a ``con``
    that already has ``httpfs`` loaded and the S3 secret created; a fresh local
    connection is used otherwise.

    min/max are stored as DuckDB's ``CAST(... AS VARCHAR)`` form, which is the
    same encoding DuckLake itself uses.
    """
    own = con is None
    eng = None
    if own:
        eng = duck_engine()
        con = eng.connect()
    try:
        schema = con.execute(text("DESCRIBE SELECT * FROM read_parquet(:p)"), {"p": path}).fetchall()
        total = con.execute(text("SELECT count(*) FROM read_parquet(:p)"), {"p": path}).scalar()
        sizes = {
            name: (int(sz) if sz is not None else None)
            for name, sz in con.execute(text(
                "SELECT path_in_schema, sum(total_compressed_size) "
                "FROM parquet_metadata(:p) GROUP BY path_in_schema"), {"p": path}).fetchall()
        }
        out = []
        for col in schema:
            name, typ = col[0], (col[1] or "").upper()
            q = '"' + name.replace('"', '""') + '"'
            vc, mn, mx = con.execute(text(
                f"SELECT count({q}), CAST(min({q}) AS VARCHAR), CAST(max({q}) AS VARCHAR) "
                "FROM read_parquet(:p)"), {"p": path}).fetchone()
            contains_nan = None
            if any(t in typ for t in ("FLOAT", "DOUBLE", "REAL")):
                nan = con.execute(text(
                    f"SELECT count(*) FILTER (WHERE isnan({q})) FROM read_parquet(:p)"),
                    {"p": path}).scalar()
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
        if own:
            con.close()
            eng.dispose()
