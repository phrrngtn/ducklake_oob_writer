"""Keep a DuckLake catalog in transaction-time order — in place, automatically.

DuckLake orders a table's state by the surrogate ``snapshot_id`` (insertion order),
not by ``snapshot_time``. A late-arriving / backfilled fact registered out of
transaction-time order therefore makes ``AT (TIMESTAMP)`` wrong. ``recanonicalize``
renumbers the snapshots so ``snapshot_id`` order matches ``snapshot_time`` order.

The shape is **generate the work, then do it** — no detect-then-branch:

1. A SQL ``ROW_NUMBER()`` computes each snapshot's canonical id (schema snapshots
   first by id, then data snapshots by ``snapshot_time``).
2. ``violators`` — the snapshots whose id actually changes — is materialised into a
   transient table. Empty when the catalog is already ordered, so every statement
   below touches zero rows: idempotency falls out, no guard.
3. The non-unique ``begin/end_snapshot`` **references** are remapped with set-based
   ``UPDATE``s (any order — DuckLake's catalog declares no foreign keys, so there is
   no dependency graph to respect).
4. The two tables whose **primary key is ``snapshot_id``** (``ducklake_snapshot`` and
   ``_snapshot_changes``) are renumbered with an offset move off the same ``violators``
   table — shift the moved ids out of range, then map them down — which permutes a
   unique key without a transient collision.

All of it is one transaction of SQLAlchemy-Core DML (portable across SQLite /
PostgreSQL / DuckDB), and it runs automatically inside ``register_data_file`` so the
catalog is kept safe without the caller having to call anything. Surrogate ids are
recomputed — time-travel by ``TIMESTAMP``, not ``VERSION``. The native reader ignores
``row_id_start`` and the allocation counters (verified by probe), and the column/name
mappings and partition specs carry no snapshot id, so the renumber is the snapshot-id
remap alone and the dimension encoding is preserved by construction.
"""
from __future__ import annotations

from loguru import logger
from sqlalchemy import (BigInteger, Column, Connection, MetaData, Table, case,
                        func, inspect, select, text, update)

from ducklake_oob_writer.catalog import DUCKLAKE_METADATA


def _renumber(conn):
    """Renumber snapshots into transaction-time order on an open ``Connection``
    (joins the caller's transaction)."""
    meta = DUCKLAKE_METADATA
    snap = meta.tables["ducklake_snapshot"]
    changes = meta.tables["ducklake_snapshot_changes"]

    # (1) canonical id per snapshot, in SQL: schema snapshots first (by id), then data
    #     snapshots by transaction-time. A "data" snapshot is one that changed data —
    #     a Parquet insert, inlined rows, or a delete (file or inlined) — per the change
    #     log (inlined inserts/deletes write no data_file row, so we read the log, not
    #     ducklake_data_file). Deletes must be ordered too, or a backfilled delete lands
    #     in the wrong place in history.
    data_snaps = select(changes.c.snapshot_id.label("sid")).where(
        changes.c.changes_made.like("inserted_into_table:%")
        | changes.c.changes_made.like("inlined_insert:%")
        | changes.c.changes_made.like("deleted_from_table:%")
        | changes.c.changes_made.like("inlined_delete:%")).distinct().subquery()
    ranked = select(
        snap.c.snapshot_id.label("old_id"),
        (func.row_number().over(order_by=[
            case((data_snaps.c.sid.isnot(None), 1), else_=0),
            case((data_snaps.c.sid.isnot(None), snap.c.snapshot_time)),
            snap.c.snapshot_id,
        ]) - 1).label("new_id"),
    ).select_from(snap.outerjoin(data_snaps, data_snaps.c.sid == snap.c.snapshot_id)).subquery()

    # (2) materialise the violators — the snapshots whose id changes. Empty ⇒ no-op.
    violators = Table("_oob_violators", MetaData(),
                      Column("old_id", BigInteger, primary_key=True),
                      Column("new_id", BigInteger),
                      prefixes=["TEMPORARY"])
    violators.drop(conn, checkfirst=True)
    violators.create(conn)
    try:
        conn.execute(violators.insert().from_select(
            ["old_id", "new_id"],
            select(ranked.c.old_id, ranked.c.new_id).where(ranked.c.old_id != ranked.c.new_id)))
        n = conn.execute(select(func.count()).select_from(violators)).scalar()
        if not n:
            return                       # already in transaction-time order — nothing to do
        logger.info("recanonicalize: renumbering {n} snapshot(s) into transaction-time order", n=n)
        moved = select(violators.c.old_id)

        # (3) references (non-unique begin/end_snapshot) — set-based remap, any order.
        for t in meta.tables.values():
            for colname in ("begin_snapshot", "end_snapshot"):
                if colname in t.c:
                    col = t.c[colname]
                    conn.execute(update(t).where(col.in_(moved)).values(
                        {colname: select(violators.c.new_id)
                                  .where(violators.c.old_id == col).scalar_subquery()}))

        # (3b) inlined data tables carry begin/end_snapshot too, but are dynamic (not in
        #      our MetaData), so remap them the same way — else an out-of-order inlined
        #      backfill would time-travel wrong after the renumber.
        if inspect(conn).has_table("ducklake_inlined_data_tables"):
            for (name,) in conn.execute(text("SELECT table_name FROM ducklake_inlined_data_tables")):
                for colname in ("begin_snapshot", "end_snapshot"):
                    conn.execute(text(
                        f"UPDATE {name} SET {colname} = "
                        f"(SELECT new_id FROM _oob_violators WHERE old_id = {colname}) "
                        f"WHERE {colname} IN (SELECT old_id FROM _oob_violators)"))

        # (4) the two snapshot_id-PK tables — permutation via offset (collision-free):
        #     shift the moved ids past the max, then map them down off `violators`.
        big = (conn.execute(select(func.max(snap.c.snapshot_id))).scalar() or 0) + 1
        for t in (snap, meta.tables["ducklake_snapshot_changes"]):
            sid = t.c.snapshot_id
            conn.execute(update(t).where(sid.in_(moved)).values(snapshot_id=sid + big))
            conn.execute(update(t).where(sid >= big).values(
                snapshot_id=select(violators.c.new_id)
                            .where(violators.c.old_id == sid - big).scalar_subquery()))
    finally:
        violators.drop(conn, checkfirst=True)


def recanonicalize(bind):
    """Renumber ``bind``'s snapshots into transaction-time order, in place.

    ``bind`` may be an ``Engine`` (a transaction is opened) or an open ``Connection``
    (the work joins the caller's transaction — how ``register_data_file`` runs it in
    the same atomic write). Idempotent; safe to call unconditionally.
    """
    if isinstance(bind, Connection):
        _renumber(bind)
    else:
        with bind.begin() as conn:
            _renumber(conn)
