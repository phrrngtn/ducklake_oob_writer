"""DuckLake replicas — give DuckLake *rows*; let it own the storage.

Both replicas ingest the result set of a tail/CDC query as **rows**, via DuckLake **inline
MVCC** — no Parquet written here, no row-map, no position delete-files. DuckLake compacts
the inline buffer to Parquet itself (``ducklake_flush_inlined_data``), and snapshot expiry
sheds old versions. *Give it rows and let it figure out the storage.*

* :class:`HistoryReplica` — ``apply_commit(ops)``: one snapshot per source commit, every
  intermediate version retained. insert → a new inline row; delete → ``end_snapshot`` the
  live row found by key; update → ``end_snapshot`` the old + inline the after-image.
* :class:`Replica` — ``apply(upserts, deletes)``: net changes (CT / CDC-as-CT), a thin
  wrapper that maps each upsert to a *merge* (supersede the live row for its key if present,
  then inline the new one) and each delete to a supersede. "Current state" is then a
  retention policy (expire old snapshots) over the same inline history.

Assumes commits/polls arrive in transaction-time order (tailing CT/CDC is monotonic). Single
key column (composite keys: join to a string first). ``AT (TIMESTAMP)`` replays the state as
of any snapshot.
"""
from __future__ import annotations

from loguru import logger
from sqlalchemy import and_, func, select

from ducklake_oob_writer import inlined


class HistoryReplica:
    """Full transaction-time history from CDC, via DuckLake inline MVCC (one snapshot per
    source commit). See the module docstring."""

    def __init__(self, writer, table_name, key_column, *, schema_name="main"):
        self.w, self.schema, self.table, self.key = writer, schema_name, table_name, key_column
        with writer.engine.connect() as conn:
            self._tid = writer._find_table_id(conn, table_name, schema_name)
            col = writer._column
            self._cols = conn.execute(
                select(col.c.column_name, col.c.column_type)
                .where(and_(col.c.table_id == self._tid, col.c.end_snapshot.is_(None)))
                .order_by(col.c.column_order)).fetchall()
        self._names = [c[0] for c in self._cols]

    def apply_commit(self, ops, *, snapshot_time=None):
        """Apply one source commit as one snapshot. ``ops`` = list (seqval order) of
        ``{"op": "I"|"U"|"D", "key": <key>, "row": {col: val}}`` (``row`` ignored for D).
        Returns snapshot id + op / inserted / superseded counts."""
        if not ops:
            return {"snapshot_id": None, "ops": 0, "inserted": 0, "superseded": 0}
        w = self.w
        tid, key = self._tid, self.key
        inserted = superseded = 0
        with w.engine.begin() as conn:
            w._load_state(conn)
            sv = w._schema_version
            name = f"ducklake_inlined_data_{tid}_{sv}"
            inlined.REGISTRY.create(conn, checkfirst=True)
            itbl = inlined.data_table(name, self._cols)
            itbl.create(conn, checkfirst=True)
            reg = inlined.REGISTRY
            if not conn.execute(select(func.count()).select_from(reg)
                                .where(reg.c.table_name == name)).scalar():
                conn.execute(reg.insert().values(table_id=tid, table_name=name, schema_version=sv))

            w._reject_if_ooo(conn, tid, snapshot_time)
            snap = w._alloc_snapshot_id()
            ts = w._table_stats
            ts_row = conn.execute(select(ts.c.record_count, ts.c.next_row_id, ts.c.file_size_bytes)
                                  .where(ts.c.table_id == tid)).fetchone()
            rc, next_row, fsz = ts_row if ts_row else (0, 0, 0)
            conn.execute(w._snapshot.insert().values(
                snapshot_id=snap, snapshot_time=snapshot_time or func.now(),
                schema_version=sv, next_catalog_id=w._next_catalog_id, next_file_id=w._next_file_id))

            kinds = set()
            for op in ops:
                k = op["op"]
                if k in ("U", "D"):            # supersede the live version (set end_snapshot)
                    r = conn.execute(itbl.update().where(and_(
                        itbl.c[key] == op["key"], itbl.c.end_snapshot.is_(None)))
                        .values(end_snapshot=snap))
                    if r.rowcount:
                        superseded += r.rowcount
                        kinds.add("inlined_delete")
                if k in ("I", "U"):            # add the new version (after-image)
                    conn.execute(itbl.insert().values(
                        row_id=next_row, begin_snapshot=snap, end_snapshot=None,
                        **{n: inlined.value(t, op["row"].get(n)) for n, t in self._cols}))
                    next_row += 1
                    rc += 1
                    inserted += 1
                    kinds.add("inlined_insert")

            vals = dict(record_count=rc, next_row_id=next_row, file_size_bytes=fsz)
            if ts_row:
                conn.execute(ts.update().where(ts.c.table_id == tid).values(**vals))
            else:
                conn.execute(ts.insert().values(table_id=tid, **vals))
            # comma-join the change kinds, insert first (matching the native update format)
            changes = ",".join(f"{kind}:{tid}" for kind in ("inlined_insert", "inlined_delete")
                               if kind in kinds)
            conn.execute(w._snapshot_changes.insert().values(
                snapshot_id=snap, changes_made=changes, author=None,
                commit_message=f"cdc commit @ {snapshot_time}", commit_extra_info=None))
            w._maybe_recanonicalize(conn)
        logger.info("{schema}.{table}: commit as snapshot {s} (+{i} / -{d})",
                    schema=self.schema, table=self.table, s=snap, i=inserted, d=superseded)
        return {"snapshot_id": snap, "ops": len(ops), "inserted": inserted, "superseded": superseded}


class Replica(HistoryReplica):
    """Current-state replica — net changes (CT, or CDC-as-CT) over the same inline MVCC.
    Every upsert is a merge (supersede the live row for its key if present, then inline the
    new one); every delete supersedes the live row. Rows go to DuckLake *as rows*; DuckLake
    owns the storage (flush the inline buffer to Parquet, expire old snapshots to shed
    history). "Current state" is a retention policy over the same inline history."""

    def apply(self, upserts=(), deletes=(), *, snapshot_time=None):
        """Apply one poll's net changes. ``upserts``: full current rows (dicts incl. the
        key); ``deletes``: keys gone from the source. Returns ``{upserted, deleted}``."""
        upserts = list(upserts)
        ops = [{"op": "D", "key": k} for k in deletes]
        ops += [{"op": "U", "key": u[self.key], "row": u} for u in upserts]
        res = self.apply_commit(ops, snapshot_time=snapshot_time)
        return {"upserted": len(upserts), "deleted": res["superseded"]}
