"""Canonicalize a DuckLake catalog into transaction-time order.

DuckLake orders a table's state by the surrogate ``snapshot_id`` (insertion
order), *not* by ``snapshot_time``. So when files are registered out of
transaction-time order — a late-arriving / backfilled fact — ``AT (TIMESTAMP)``
time-travel returns wrong results (the backfill's snapshot sits *above* the
later-dated data it should precede). See ``docs/Temporal and Partitioning
Design.md`` for the full reasoning and the empirical demonstration.

``recanonicalize`` rebuilds the catalog metadata so ``snapshot_id`` order matches
``snapshot_time`` (transaction-time) order — the *one* canonical arrangement. It
is a pure, deterministic function of the data: read the registered files and
their transaction-times, sort by ``(snapshot_time, path)``, and replay. The data
files themselves are never touched (they are referenced by path); only catalog
metadata is rebuilt, reusing the per-file stats already in the source catalog so
no Parquet is re-read.

Note: this rewrites every ``snapshot_id``. Surrogates carry no meaning outside the
database and are expected to change — clients should time-travel by ``TIMESTAMP``
(stable), not by ``VERSION`` (a derived ordinal that this operation recomputes).
"""
from __future__ import annotations

import datetime as _dt

from sqlalchemy import text

from ducklake_oob_writer.catalog import create_catalog
from ducklake_oob_writer.writer import DuckLakeWriter


def _parse_ts(v):
    if isinstance(v, _dt.datetime):
        return v
    return _dt.datetime.fromisoformat(str(v).replace("+00:00", ""))


def _read_state(engine):
    """Read the current (non-deleted) schema, partition specs, and data files —
    each file with its transaction-time and reconstructed column stats."""
    with engine.connect() as c:
        data_path = c.execute(text(
            "SELECT value FROM ducklake_metadata WHERE key = 'data_path'")).scalar()

        tables = c.execute(text(
            "SELECT t.table_id, s.schema_name, t.table_name "
            "FROM ducklake_table t JOIN ducklake_schema s ON t.schema_id = s.schema_id "
            "WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL "
            "ORDER BY t.table_id")).fetchall()

        cols, partcols = {}, {}
        for tid, sname, tname in tables:
            colrows = c.execute(text(
                "SELECT column_id, column_name, column_type FROM ducklake_column "
                "WHERE table_id = :t AND end_snapshot IS NULL ORDER BY column_order"),
                {"t": tid}).fetchall()
            cols[tid] = (sname, tname, [(n, ty) for _, n, ty in colrows])
            id2name = {cid: n for cid, n, _ in colrows}
            pc = c.execute(text(
                "SELECT pc.column_id FROM ducklake_partition_column pc "
                "JOIN ducklake_partition_info pi ON pc.partition_id = pi.partition_id "
                "WHERE pc.table_id = :t AND pi.end_snapshot IS NULL "
                "ORDER BY pc.partition_key_index"), {"t": tid}).fetchall()
            partcols[tid] = [id2name[cid] for (cid,) in pc if cid in id2name]

        files = []
        for fid, tid, path, rc, fsz, foot, st in c.execute(text(
                "SELECT df.data_file_id, df.table_id, df.path, df.record_count, "
                "df.file_size_bytes, df.footer_size, s.snapshot_time "
                "FROM ducklake_data_file df "
                "JOIN ducklake_snapshot s ON df.begin_snapshot = s.snapshot_id "
                "WHERE df.end_snapshot IS NULL")).fetchall():
            statrows = c.execute(text(
                "SELECT col.column_name, fcs.min_value, fcs.max_value, fcs.value_count, "
                "fcs.null_count, fcs.column_size_bytes, fcs.contains_nan "
                "FROM ducklake_file_column_stats fcs "
                "JOIN ducklake_column col ON fcs.column_id = col.column_id "
                "WHERE fcs.data_file_id = :f AND col.end_snapshot IS NULL"),
                {"f": fid}).fetchall()
            cstats = {n: {"min_value": mn, "max_value": mx, "value_count": vc,
                          "null_count": nc, "column_size_bytes": csz, "contains_nan": cn}
                      for n, mn, mx, vc, nc, csz, cn in statrows}
            files.append({"table_id": tid, "path": path, "record_count": rc,
                          "file_size_bytes": fsz, "footer_size": foot,
                          "transaction_time": _parse_ts(st),
                          "column_stats": cstats or None})
    return data_path, cols, partcols, files


def recanonicalize(source_engine, target_engine):
    """Rebuild ``source_engine``'s catalog into ``target_engine`` with snapshots in
    transaction-time order, so ``AT (TIMESTAMP)`` is correct regardless of the
    order files were originally registered. Deterministic — ties on
    ``snapshot_time`` are broken by ``path``. Returns the target ``MetaData``."""
    data_path, cols, partcols, files = _read_state(source_engine)

    meta = create_catalog(target_engine)
    w = DuckLakeWriter(target_engine, meta)
    w.init_catalog(data_path=data_path)

    for tid in sorted(cols):
        sname, tname, columns = cols[tid]
        w.create_table(sname, tname, columns)
        if partcols.get(tid):
            w.set_partitioning(tname, partcols[tid], schema_name=sname)

    name_of = {tid: cols[tid] for tid in cols}
    for f in sorted(files, key=lambda f: (f["transaction_time"], f["path"])):
        sname, tname, _ = name_of[f["table_id"]]
        w.register_data_file(
            tname, path=f["path"], record_count=f["record_count"],
            file_size_bytes=f["file_size_bytes"], footer_size=f["footer_size"],
            schema_name=sname, column_stats=f["column_stats"],
            snapshot_time=f["transaction_time"])
    return meta
