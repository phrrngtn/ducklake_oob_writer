"""Canonicalize a DuckLake catalog into transaction-time order — in place.

DuckLake orders a table's state by the surrogate ``snapshot_id`` (insertion order),
not by ``snapshot_time``. A late-arriving / backfilled fact registered out of
transaction-time order therefore makes ``AT (TIMESTAMP)`` wrong (the backfill's high
``snapshot_id`` makes its cumulative state include later-dated data). ``recanonicalize``
renumbers the snapshots so ``snapshot_id`` order matches ``snapshot_time`` order.

It is **idempotent and in place**: one transaction, set-based, expressed with
SQLAlchemy Core so a single implementation works across SQLite / PostgreSQL / DuckDB
(no dialect-specific SQL). When the catalog is already ordered it is a cheap no-op (a
single inversion-count query). Only the *moved* snapshots and the references to them
are touched — the impacted slice; unchanged rows are never written.

Surrogate ids are recomputed — clients time-travel by ``TIMESTAMP``, not ``VERSION``.
The native reader does not depend on ``row_id_start`` or the per-snapshot allocation
counters (``next_file_id`` / ``next_catalog_id`` / ``schema_version`` — verified by
probe: corrupting them leaves reads and time-travel correct), so the renumber is the
snapshot-id remap alone. Column/name mappings and partition specs reference no
snapshot id, so they are untouched and survive automatically.
"""
from __future__ import annotations

import datetime as _dt

from sqlalchemy import and_, case, delete, func, insert, select, update

from ducklake_oob_writer.catalog import DUCKLAKE_METADATA


def _parse_ts(v):
    if v is None or isinstance(v, _dt.datetime):
        return v
    return _dt.datetime.fromisoformat(str(v).replace("+00:00", ""))


def recanonicalize(engine):
    """Renumber ``engine``'s snapshots in place so ``snapshot_id`` order matches
    ``snapshot_time`` order. Idempotent (a no-op when already ordered), one
    transaction, set-based via SQLAlchemy Core. Safe to call unconditionally after
    every write — most of the time it does nothing but the inversion check."""
    meta = DUCKLAKE_METADATA
    snap = meta.tables["ducklake_snapshot"]
    dfile = meta.tables["ducklake_data_file"]

    with engine.begin() as conn:
        # Idempotent guard: is any DATA snapshot out of order? (a lower snapshot_id
        # carrying a later snapshot_time than a higher one). One set-based query.
        ds = (select(snap.c.snapshot_id.label("sid"), snap.c.snapshot_time.label("st"))
              .where(snap.c.snapshot_id.in_(select(dfile.c.begin_snapshot)))).subquery()
        a, b = ds.alias("a"), ds.alias("b")
        if not conn.execute(select(func.count()).select_from(
                a.join(b, and_(a.c.sid < b.c.sid, a.c.st > b.c.st)))).scalar():
            return

        # Canonical order: schema snapshots first (by id), then data by transaction-time.
        data_ids = {r[0] for r in conn.execute(select(dfile.c.begin_snapshot).distinct())}
        snaps = conn.execute(select(snap)).mappings().all()
        order = sorted(snaps, key=lambda r: (
            1 if r["snapshot_id"] in data_ids else 0,
            _parse_ts(r["snapshot_time"]) if r["snapshot_id"] in data_ids else _dt.datetime.min,
            r["snapshot_id"]))
        new_of = {r["snapshot_id"]: new for new, r in enumerate(order)}
        changed = {old: new for old, new in new_of.items() if old != new}
        if not changed:
            return

        changed_ids = list(changed)
        # Tables KEYED by snapshot_id (unique: ducklake_snapshot, _snapshot_changes) —
        # renumber via delete+reinsert. The moved ids permute among their own set (a
        # bijection's non-fixed points), so after deleting the old ones the new ones are
        # free: no transient unique-constraint collision.
        for t in meta.tables.values():
            if "snapshot_id" not in t.c:
                continue
            rows = conn.execute(
                select(t).where(t.c.snapshot_id.in_(changed_ids))).mappings().all()
            conn.execute(delete(t).where(t.c.snapshot_id.in_(changed_ids)))
            if rows:
                conn.execute(insert(t), [{**dict(r),
                             "snapshot_id": changed[r["snapshot_id"]]} for r in rows])
        # Non-unique REFERENCES (begin_snapshot / end_snapshot) — remap in place with a
        # CASE over just the moved ids; everything else is left untouched.
        for t in meta.tables.values():
            cols = [c.name for c in t.columns if c.name in ("begin_snapshot", "end_snapshot")]
            if cols:
                conn.execute(update(t).values({
                    c: case(*[(t.c[c] == old, new) for old, new in changed.items()],
                            else_=t.c[c]) for c in cols}))
