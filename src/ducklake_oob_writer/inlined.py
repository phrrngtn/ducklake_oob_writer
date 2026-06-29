"""DuckLake inlined data tables as SQLAlchemy Core objects.

The inlined tables are dynamic (``ducklake_inlined_data_<table_id>_<schema_version>``),
but a runtime ``Table`` lets us drive them with the expression language + bound
parameters — no embedded SQL — exactly like the static catalog. Column types are chosen
to render DuckLake's native inlined storage (BIGINT / DOUBLE / VARCHAR / BOOLEAN), so an
OOB-built inlined table is byte-identical to one DuckDB writes.
"""
from __future__ import annotations

from sqlalchemy import BigInteger, Boolean, Column, Double, MetaData, String, Table

# DuckLake scalar types the metadata backend stores natively (else VARCHAR text).
_NATIVE = {
    "BIGINT": "BIGINT", "INTEGER": "BIGINT", "INT": "BIGINT", "SMALLINT": "BIGINT",
    "TINYINT": "BIGINT", "HUGEINT": "BIGINT",
    "INT8": "BIGINT", "INT16": "BIGINT", "INT32": "BIGINT", "INT64": "BIGINT",
    "UINT8": "BIGINT", "UINT16": "BIGINT", "UINT32": "BIGINT", "UINT64": "BIGINT",
    "UBIGINT": "BIGINT", "UINTEGER": "BIGINT", "USMALLINT": "BIGINT", "UTINYINT": "BIGINT",
    "DOUBLE": "DOUBLE", "FLOAT": "DOUBLE", "REAL": "DOUBLE",
    "FLOAT4": "DOUBLE", "FLOAT8": "DOUBLE", "FLOAT32": "DOUBLE", "FLOAT64": "DOUBLE",
    "VARCHAR": "VARCHAR", "TEXT": "VARCHAR", "STRING": "VARCHAR",
    "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN",
}
_TEXT = ("DECIMAL", "NUMERIC", "DATE", "TIMESTAMP", "TIME")
_SA = {"BIGINT": BigInteger, "DOUBLE": Double, "VARCHAR": String, "BOOLEAN": Boolean}


def _base(ducklake_type: str) -> str:
    return ducklake_type.upper().split("(", 1)[0].strip()


def is_nested(ducklake_type: str) -> bool:
    u = ducklake_type.upper()
    return u.endswith("[]") or any(t in u for t in ("STRUCT", "MAP", "UNION", "LIST"))


def storage_type(ducklake_type: str) -> str:
    """Backend storage type (string) for an inlined column."""
    base = _base(ducklake_type)
    if base in _NATIVE:
        return _NATIVE[base]
    if base in _TEXT:
        return "VARCHAR"
    raise ValueError(f"inlined storage: unsupported column type {ducklake_type!r} — inline "
                     f"only scalar/simple types; register such a table as Parquet instead")


def value(ducklake_type: str, v):
    """Format a value for its inlined column: native scalars as-is, the rest as DuckLake's
    canonical text (which ``str()`` yields)."""
    if v is None:
        return None
    return v if _base(ducklake_type) in _NATIVE else str(v)


# the registry that maps a table_id to its inlined data table
REGISTRY = Table("ducklake_inlined_data_tables", MetaData(),
                 Column("table_id", BigInteger), Column("table_name", String),
                 Column("schema_version", BigInteger))


def data_table(name: str, columns) -> Table:
    """A SQLAlchemy ``Table`` for ``ducklake_inlined_data_<tid>_<sv>``: the MVCC
    bookkeeping (``row_id`` / ``begin_snapshot`` / ``end_snapshot``) plus the table's
    columns, typed to render DuckLake's native storage types. ``columns`` = ``[(name,
    ducklake_type), …]``."""
    return Table(name, MetaData(),
                 Column("row_id", BigInteger),
                 Column("begin_snapshot", BigInteger),
                 Column("end_snapshot", BigInteger),
                 *[Column(n, _SA[storage_type(t)]) for n, t in columns])
