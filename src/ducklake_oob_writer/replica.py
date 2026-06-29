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
                        insert as _insert, select, text)

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
            self._data_path = conn.execute(text(
                "SELECT value FROM ducklake_metadata WHERE key='data_path'")).scalar()
        self._names = [c[0] for c in self._cols]
        self._scope = and_(ROWMAP.c.schema_name == schema_name, ROWMAP.c.table_name == table_name)

    def _rel_path(self, data_file_id):
        with self.w.engine.connect() as conn:
            return conn.execute(text("SELECT path FROM ducklake_data_file WHERE data_file_id = :d"),
                                {"d": data_file_id}).scalar()

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
