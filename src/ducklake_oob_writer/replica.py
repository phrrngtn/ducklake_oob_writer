"""Current-state replica — apply net changes (CT, or CDC treated as CT) to keep a
DuckLake table mirroring a source's *current* state.

The source is polled for **net** changes since a watermark; each poll yields
``upserts`` (the full current row for every inserted/updated key) and ``deletes`` (keys
gone from the source). We don't replay intermediate values — current state only — so the
apply is a **merge**: supersede the old row for every changed/deleted key, then register
the new rows. A small ``oob_replica_rowmap`` (key → data_file_id, position) lets an
update/delete find the physical row to remove (a DuckLake position delete-file).

Snapshot_time should be monotonic (the poll/watermark time) — tailing CT/CDC is in
transaction-time order, so this Just Works; an out-of-order delete is refused upstream.
Needs ``duckdb`` (writes/reads the Parquet). Single key column (composite keys: join to a
string before calling). The replica keeps full transaction-time history — `AT (TIMESTAMP)`
shows the state as of any poll.
"""
from __future__ import annotations

import os
from uuid import uuid4

from loguru import logger
from sqlalchemy import (BigInteger, Column, MetaData, Table, Text, and_, delete as _delete,
                        func, insert as _insert, select)

from ducklake_oob_writer import inlined
from ducklake_oob_writer.canonicalize import recanonicalize

_META = MetaData()
ROWMAP = Table("oob_replica_rowmap", _META,
               Column("schema_name", Text), Column("table_name", Text), Column("k", Text),
               Column("data_file_id", BigInteger), Column("pos", BigInteger))

_DUCKDB_TYPE = {
    "int8": "TINYINT", "int16": "SMALLINT", "int32": "INTEGER", "int64": "BIGINT",
    "integer": "BIGINT", "bigint": "BIGINT", "smallint": "SMALLINT", "tinyint": "TINYINT",
    "float32": "FLOAT", "float64": "DOUBLE", "float": "DOUBLE", "double": "DOUBLE",
    "varchar": "VARCHAR", "string": "VARCHAR", "boolean": "BOOLEAN", "bool": "BOOLEAN",
    "date": "DATE", "timestamp": "TIMESTAMP", "time": "TIME",
}


def _ddt(ducklake_type: str) -> str:
    base = ducklake_type.split("(", 1)[0].strip().lower()
    if base in ("decimal", "numeric"):
        return ducklake_type.upper()
    return _DUCKDB_TYPE.get(base, "VARCHAR")


class Replica:
    def __init__(self, writer, table_name, key_column, *, schema_name="main"):
        self.w, self.schema, self.table, self.key = writer, schema_name, table_name, key_column
        ROWMAP.create(writer.engine, checkfirst=True)
        with writer.engine.connect() as conn:
            tid = writer._find_table_id(conn, table_name, schema_name)
            col = writer._column
            self._cols = conn.execute(
                select(col.c.column_name, col.c.column_type)
                .where(and_(col.c.table_id == tid, col.c.end_snapshot.is_(None)))
                .order_by(col.c.column_order)).fetchall()
            self._data_path = writer._data_path(conn)
        self._names = [c[0] for c in self._cols]
        self._scope = and_(ROWMAP.c.schema_name == schema_name, ROWMAP.c.table_name == table_name)

    def _rel_path(self, data_file_id):
        df = self.w._data_file
        with self.w.engine.connect() as conn:
            return conn.execute(select(df.c.path)
                                .where(df.c.data_file_id == data_file_id)).scalar()

    def apply(self, upserts=(), deletes=(), *, snapshot_time=None):
        """Apply one poll's net changes. ``upserts``: full current rows (dicts incl. the key
        column); ``deletes``: key values gone from the source. Returns counts."""
        upserts = list(upserts)
        eng = self.w.engine
        del_keys = {str(k) for k in deletes}
        ups_keys = [str(u[self.key]) for u in upserts]

        # which upsert keys already exist => updates, whose old row must be superseded too
        with eng.connect() as conn:
            existing = {r[0] for r in conn.execute(
                select(ROWMAP.c.k).where(and_(self._scope, ROWMAP.c.k.in_(ups_keys))))} if ups_keys else set()
        remove = del_keys | (set(ups_keys) & existing)

        # 1. supersede the old rows: find their (file, position) via the row-map, delete them
        if remove:
            with eng.connect() as conn:
                locs = conn.execute(select(ROWMAP.c.k, ROWMAP.c.data_file_id, ROWMAP.c.pos)
                                    .where(and_(self._scope, ROWMAP.c.k.in_(list(remove))))).fetchall()
            by_file = {}
            for _k, fid, pos in locs:
                by_file.setdefault(fid, []).append(pos)
            for fid, positions in by_file.items():
                self.w.delete_rows(self.table, self._rel_path(fid), positions,
                                   schema_name=self.schema, snapshot_time=snapshot_time)
            with eng.begin() as conn:
                conn.execute(_delete(ROWMAP).where(and_(self._scope, ROWMAP.c.k.in_(list(remove)))))

        # 2. register the new rows as a Parquet file; row-map their *physical* positions
        if upserts:
            import duckdb
            tdir = os.path.join(self._data_path, self.schema, self.table)
            os.makedirs(tdir, exist_ok=True)
            rel = f"upsert-{uuid4().hex[:8]}.parquet"
            pq = os.path.join(tdir, rel)
            d = duckdb.connect()
            d.execute("CREATE TABLE stg (" + ", ".join(f'"{n}" {_ddt(t)}' for n, t in self._cols) + ")")
            d.executemany("INSERT INTO stg VALUES (" + ",".join("?" * len(self._names)) + ")",
                          [tuple(u.get(n) for n in self._names) for u in upserts])
            d.execute(f"COPY stg TO '{pq}' (FORMAT PARQUET)")
            info = self.w.register_parquet(self.table, pq, rel_path=rel,
                                           schema_name=self.schema, snapshot_time=snapshot_time)
            fid = info["data_file_id"]
            # read back the actual physical positions per key (COPY may reorder)
            posmap = {str(k): int(p) for p, k in d.execute(
                f'SELECT file_row_number, "{self.key}" FROM read_parquet(\'{pq}\', file_row_number=true)').fetchall()}
            d.close()
            with eng.begin() as conn:
                conn.execute(_insert(ROWMAP), [
                    {"schema_name": self.schema, "table_name": self.table,
                     "k": str(u[self.key]), "data_file_id": fid, "pos": posmap[str(u[self.key])]}
                    for u in upserts])

        logger.info("replica {schema}.{table}: +{up} upserts, -{rm} removed",
                    schema=self.schema, table=self.table, up=len(upserts), rm=len(remove))
        return {"upserted": len(upserts), "deleted": len(remove)}


class HistoryReplica:
    """Full transaction-time history from CDC — every intermediate version retained, via
    DuckLake **inline MVCC**.

    One snapshot **per source commit** (the changes of a commit are stapled together by
    their LSN, stamped with the commit's transaction-time). Within a commit, in seqval
    order: insert → a new inline row (``begin_snapshot`` = the commit); delete →
    ``end_snapshot`` the live row found by key; update → ``end_snapshot`` the old + insert
    the after-image. ``AT (TIMESTAMP)`` then replays to the state after any commit, and the
    full version list of a key is just ``WHERE key = … ORDER BY begin_snapshot``.

    Inline-only (the catalog DB) — ideal for the small/frequent CDC stream; flush to
    Parquet natively (``ducklake_flush_inlined_data``) for volume. Assumes commits arrive
    in transaction-time order (tailing CDC is monotonic). Single key column.
    """

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

    def apply_commit(self, ops, *, snapshot_time):
        """Apply one source commit as one snapshot. ``ops`` is a list (in seqval order) of
        dicts ``{"op": "I"|"U"|"D", "key": <key value>, "row": {col: val}}`` — ``row`` is
        the after-image for I/U, ignored for D. Returns the snapshot id + op count."""
        w = self.w
        tid, key = self._tid, self.key
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
                    conn.execute(itbl.update().where(and_(
                        itbl.c[key] == op["key"], itbl.c.end_snapshot.is_(None)))
                        .values(end_snapshot=snap))
                    kinds.add("inlined_delete")
                if k in ("I", "U"):            # add the new version (after-image)
                    conn.execute(itbl.insert().values(
                        row_id=next_row, begin_snapshot=snap, end_snapshot=None,
                        **{n: inlined.value(t, op["row"].get(n)) for n, t in self._cols}))
                    next_row += 1
                    rc += 1
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
            recanonicalize(conn)
        logger.info("history {schema}.{table}: applied commit ({n} ops) as snapshot {s}",
                    schema=self.schema, table=self.table, n=len(ops), s=snap)
        return {"snapshot_id": snap, "ops": len(ops)}
