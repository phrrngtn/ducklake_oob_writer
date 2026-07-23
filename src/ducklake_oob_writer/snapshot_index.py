"""Ancillary per-snapshot → table index (for the reject_out_of_order guard).

Like ``oob_incorporation``, this is the OOB writer's **own** table in the catalog database —
**not** part of the ``ducklake_*`` catalog. The two clients that read the catalog are (1) DuckDB's
``ducklake`` extension, which reads only the ``ducklake_*`` tables and ignores this one, and (2) the
OOB writer, which owns it. So we are free to record whatever structured metadata we need to support
OOB writing here.

It records, for each temporal snapshot the writer creates in ``reject_out_of_order`` mode, which
table it touched and its ``snapshot_time`` — a **structured** replacement for the per-table
attribution DuckLake otherwise encodes as delimited text in
``ducklake_snapshot_changes.changes_made``. With it, the monotonicity guard is a plain indexed
query (``has_later``) instead of string-parsing DuckLake's own change log — the string format stays
confined to *emitting* what the extension expects, never to *reading* it.

Only maintained when ``reject_out_of_order`` is set, so default-mode writers are byte-unchanged.
"""
from __future__ import annotations

from sqlalchemy import (BigInteger, Column, DateTime, Index, MetaData, Table, and_, func,
                        select)

_META = MetaData()
OOB_SNAPSHOT = Table(
    "oob_snapshot_table", _META,
    Column("snapshot_id", BigInteger, nullable=False),
    Column("table_id", BigInteger, nullable=False),
    Column("snapshot_time", DateTime(timezone=True)),
    Index("ix_oob_snapshot_table", "table_id", "snapshot_time"),
)


def create_snapshot_index(bind):
    """Create the ``oob_snapshot_table`` index if absent (idempotent)."""
    OOB_SNAPSHOT.create(bind, checkfirst=True)


def record_snapshot_table(conn, snapshot_id, table_id, snapshot_time):
    """Record that ``snapshot_id`` touched ``table_id`` at ``snapshot_time`` (caller's txn)."""
    create_snapshot_index(conn)
    conn.execute(OOB_SNAPSHOT.insert().values(
        snapshot_id=snapshot_id, table_id=table_id, snapshot_time=snapshot_time))


def has_later(conn, table_id, snapshot_time):
    """True iff a snapshot for ``table_id`` already exists with a strictly later
    ``snapshot_time`` — i.e. the incoming write would be out of transaction-time order for
    that table. Per-table: other tables' snapshots are not considered."""
    create_snapshot_index(conn)
    return conn.execute(
        select(func.count()).select_from(OOB_SNAPSHOT).where(and_(
            OOB_SNAPSHOT.c.table_id == table_id,
            OOB_SNAPSHOT.c.snapshot_time > snapshot_time))).scalar() > 0
