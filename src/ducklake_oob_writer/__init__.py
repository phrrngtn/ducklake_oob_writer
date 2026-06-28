"""ducklake_oob_writer — out-of-band (OOB) writer for DuckLake catalogs.

You write the Parquet data files yourself (via duckdb, pyarrow, whatever), then
register them *directly* into the DuckLake catalog tables through SQLAlchemy —
never going through DuckDB's `ducklake` extension write path. The catalog can be
backed by PostgreSQL, SQLite, or DuckDB.

Public API:
    create_catalog(engine, schema=None)   -- bootstrap the ~29 catalog tables
    _build_metadata(schema=None)          -- SA MetaData describing those tables
    DUCKLAKE_METADATA                      -- default (unqualified) MetaData instance
    DUCKLAKE_VERSION                       -- catalog protocol version string
    DuckLakeWriter(engine, meta)           -- OOB writer (create_table/register_data_file/...)

Extracted verbatim from rule4 so rule4 and ha_ducklake share one implementation.
"""
from ducklake_oob_writer.catalog import (
    DUCKLAKE_METADATA,
    DUCKLAKE_VERSION,
    _build_metadata,
    create_catalog,
)
from ducklake_oob_writer.maintenance import (
    attach_lake,
    cleanup_old_files,
    compact,
    expire_snapshots,
    run_maintenance,
)
from ducklake_oob_writer.canonicalize import recanonicalize
from ducklake_oob_writer.parquet import footer_and_size
from ducklake_oob_writer.writer import DuckLakeWriter

__all__ = [
    "DUCKLAKE_METADATA",
    "DUCKLAKE_VERSION",
    "_build_metadata",
    "create_catalog",
    "DuckLakeWriter",
    "recanonicalize",
    "footer_and_size",
    # maintenance (delegated to DuckLake's native engine; needs the `duckdb` extra)
    "attach_lake",
    "compact",
    "expire_snapshots",
    "cleanup_old_files",
    "run_maintenance",
]
