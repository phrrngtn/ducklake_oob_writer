"""Content-hash incorporation log + knowledge-time projection.

The OOB writer's own ancillary record of *what* was incorporated and *when* — kept
in a table (``oob_incorporation``) that is **not** part of the ``ducklake_*`` catalog
(which is a derived view). It is bitemporal:

- ``transaction_time`` — the *source's* clock (the same value stamped as the DuckLake
  ``snapshot_time``); non-monotonic, since backfills arrive out of order.
- ``incorporation_time`` / ``incorporation_seq`` — *our* clock: when we wrote it.
  ``incorporation_seq`` is monotonic, so it's the tailing cursor (and the axis a
  derived/“tailing” consumer should advance on, never ``transaction_time``).

Each row is keyed for content-addressing by ``content_hash`` and points at the
``data_file_id`` it produced (stable across canonicalization — only ``snapshot_id`` is
renumbered).

``lake_as_known_at(cutoff)`` projects "the lake as we knew it at incorporation N": a
catalog over the **same immutable Parquet** containing only the files incorporated up
to the cutoff, in transaction-time order. Metadata only — zero data copied.
"""
from __future__ import annotations

import datetime as _dt

from loguru import logger
from sqlalchemy import (BigInteger, Column, DateTime, MetaData, Table, Text, func,
                        select, text)

from ducklake_oob_writer.catalog import create_catalog

_META = MetaData()
INCORPORATION_LOG = Table(
    "oob_incorporation", _META,
    Column("incorporation_seq", BigInteger, primary_key=True, autoincrement=False),
    Column("content_hash", Text),
    Column("source_uri", Text),
    Column("transaction_time", DateTime(timezone=True)),
    Column("incorporation_time", DateTime(timezone=True)),
    Column("schema_name", Text),
    Column("table_name", Text),
    Column("data_file_id", BigInteger),
)


def create_incorporation_log(bind):
    """Create the ``oob_incorporation`` table if absent (idempotent)."""
    INCORPORATION_LOG.create(bind, checkfirst=True)


def already_incorporated(conn, content_hash):
    """True if an artifact with this content hash is already in the log (dedup key)."""
    return conn.execute(select(func.count()).select_from(INCORPORATION_LOG)
                        .where(INCORPORATION_LOG.c.content_hash == content_hash)).scalar() > 0


def record_incorporation(conn, *, content_hash, source_uri, transaction_time,
                         data_file_id, schema_name, table_name, incorporation_time=None):
    """Append one row to the incorporation log on an open connection (the caller's
    transaction). ``incorporation_seq`` is the next monotonic value; ``incorporation_time``
    defaults to now. Returns the assigned ``incorporation_seq``."""
    create_incorporation_log(conn)
    nxt = conn.execute(select(func.max(INCORPORATION_LOG.c.incorporation_seq))).scalar()
    seq = 0 if nxt is None else nxt + 1
    conn.execute(INCORPORATION_LOG.insert().values(
        incorporation_seq=seq, content_hash=content_hash, source_uri=source_uri,
        transaction_time=transaction_time,
        incorporation_time=incorporation_time or _dt.datetime.now(_dt.timezone.utc),
        schema_name=schema_name, table_name=table_name, data_file_id=data_file_id))
    logger.debug(
        "incorporated {schema_name}.{table_name} <- {digest}… as seq {seq}",
        schema_name=schema_name, table_name=table_name,
        digest=(content_hash or "")[:12], seq=seq)
    return seq


def _parse_ts(v):
    if v is None or isinstance(v, _dt.datetime):
        return v
    return _dt.datetime.fromisoformat(str(v).replace("+00:00", ""))


def lake_as_known_at(source_engine, target_engine, *, seq=None, incorporation_time=None):
    """Project "the lake as we knew it at incorporation `cutoff`" into a fresh catalog.

    Exactly one of ``seq`` / ``incorporation_time`` is the cutoff. The projection
    contains only the data files whose incorporation is ``<=`` the cutoff, in
    transaction-time order, over the *same* Parquet files (nothing is copied). Returns
    the set of ``data_file_id``s included.
    """
    if (seq is None) == (incorporation_time is None):
        raise ValueError("pass exactly one of seq= or incorporation_time=")
    log = INCORPORATION_LOG
    cut = (log.c.incorporation_seq <= seq if seq is not None
           else log.c.incorporation_time <= incorporation_time)

    with source_engine.connect() as src:
        data_path = src.execute(text(
            "SELECT value FROM ducklake_metadata WHERE key = 'data_path'")).scalar()
        included = {r[0] for r in src.execute(select(log.c.data_file_id).where(cut))}

        # schema (current tables + columns) and any partition specs
        tables = src.execute(text(
            "SELECT t.table_id, s.schema_name, t.table_name "
            "FROM ducklake_table t JOIN ducklake_schema s ON t.schema_id = s.schema_id "
            "WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL ORDER BY t.table_id")).fetchall()
        cols, partcols = {}, {}
        for tid, sname, tname in tables:
            colrows = src.execute(text(
                "SELECT column_id, column_name, column_type FROM ducklake_column "
                "WHERE table_id = :t AND end_snapshot IS NULL ORDER BY column_order"), {"t": tid}).fetchall()
            cols[tid] = (sname, tname, [(n, ty) for _, n, ty in colrows])
            id2name = {cid: n for cid, n, _ in colrows}
            pc = src.execute(text(
                "SELECT pc.column_id FROM ducklake_partition_column pc "
                "JOIN ducklake_partition_info pi ON pc.partition_id = pi.partition_id "
                "WHERE pc.table_id = :t AND pi.end_snapshot IS NULL "
                "ORDER BY pc.partition_key_index"), {"t": tid}).fetchall()
            partcols[tid] = [id2name[cid] for (cid,) in pc if cid in id2name]

        # the included files, with stats + transaction-time + mapping
        files = []
        for fid, tid, path, rc, fsz, foot, st, mid in src.execute(text(
                "SELECT df.data_file_id, df.table_id, df.path, df.record_count, "
                "df.file_size_bytes, df.footer_size, s.snapshot_time, df.mapping_id "
                "FROM ducklake_data_file df "
                "JOIN ducklake_snapshot s ON df.begin_snapshot = s.snapshot_id "
                "WHERE df.end_snapshot IS NULL")).fetchall():
            if fid not in included:
                continue
            stats = {n: {"min_value": mn, "max_value": mx, "value_count": vc,
                         "null_count": nc, "column_size_bytes": csz, "contains_nan": cn}
                     for n, mn, mx, vc, nc, csz, cn in src.execute(text(
                        "SELECT col.column_name, fcs.min_value, fcs.max_value, fcs.value_count, "
                        "fcs.null_count, fcs.column_size_bytes, fcs.contains_nan "
                        "FROM ducklake_file_column_stats fcs "
                        "JOIN ducklake_column col ON fcs.column_id = col.column_id "
                        "WHERE fcs.data_file_id = :f AND col.end_snapshot IS NULL"), {"f": fid}).fetchall()}
            files.append({"table_id": tid, "path": path, "record_count": rc,
                          "file_size_bytes": fsz, "footer_size": foot,
                          "transaction_time": _parse_ts(st), "stats": stats or None, "mapping_id": mid})

        logger.info(
            "lake_as_known_at({cut}): projecting {n} file(s) over the shared Parquet",
            cut=(f"seq<={seq}" if seq is not None else f"time<={incorporation_time}"),
            n=len(included))

        mappings = {}
        for mid, mtid, mtype in src.execute(text(
                "SELECT mapping_id, table_id, type FROM ducklake_column_mapping")).fetchall():
            nm = src.execute(text(
                "SELECT nm.source_name, col.column_name, nm.is_partition "
                "FROM ducklake_name_mapping nm JOIN ducklake_column col ON nm.target_field_id = col.column_id "
                "WHERE nm.mapping_id = :m AND col.end_snapshot IS NULL ORDER BY nm.column_id"), {"m": mid}).fetchall()
            mappings[mid] = {"table_id": mtid, "type": mtype,
                             "cols": [(sn, cn, bool(isp)) for sn, cn, isp in nm]}

    # build the projection catalog (register in transaction-time order, so the
    # auto-canonicalize in register_data_file is a no-op each time)
    from ducklake_oob_writer.writer import DuckLakeWriter

    meta = create_catalog(target_engine)
    w = DuckLakeWriter(target_engine, meta)
    w.init_catalog(data_path=data_path)
    tgt_table, field_of = {}, {}
    for tid in sorted(cols):
        sname, tname, columns = cols[tid]
        res = w.create_table(sname, tname, columns)
        tgt_table[tid] = res["table_id"]
        field_of[res["table_id"]] = dict(zip([n for n, _ in columns], res["column_ids"]))
        if partcols.get(tid):
            w.set_partitioning(tname, partcols[tid], schema_name=sname)

    old_to_new_mid = {}
    with target_engine.begin() as tc:
        for new_mid, old_mid in enumerate(sorted(m for m in mappings
                                                 if any(f["mapping_id"] == m for f in files))):
            m = mappings[old_mid]
            if m["table_id"] not in tgt_table:
                continue
            old_to_new_mid[old_mid] = new_mid
            tgt_tid = tgt_table[m["table_id"]]
            tc.execute(text("INSERT INTO ducklake_column_mapping(mapping_id, table_id, type) "
                            "VALUES (:m,:t,:ty)"), {"m": new_mid, "t": tgt_tid, "ty": m["type"]})
            for idx, (sn, cn, isp) in enumerate(m["cols"]):
                tc.execute(text("INSERT INTO ducklake_name_mapping(mapping_id, column_id, source_name, "
                                "target_field_id, parent_column, is_partition) VALUES (:m,:c,:s,:f,NULL,:p)"),
                           {"m": new_mid, "c": idx, "s": sn, "f": field_of[tgt_tid][cn], "p": isp})

    name_of = {tid: cols[tid] for tid in cols}
    for f in sorted(files, key=lambda f: (f["transaction_time"], f["path"])):
        sname, tname, _ = name_of[f["table_id"]]
        info = w.register_data_file(
            tname, path=f["path"], record_count=f["record_count"],
            file_size_bytes=f["file_size_bytes"], footer_size=f["footer_size"],
            schema_name=sname, column_stats=f["stats"], snapshot_time=f["transaction_time"])
        if f["mapping_id"] is not None and f["mapping_id"] in old_to_new_mid:
            with target_engine.begin() as tc:
                tc.execute(text("UPDATE ducklake_data_file SET mapping_id = :m WHERE data_file_id = :d"),
                           {"m": old_to_new_mid[f["mapping_id"]], "d": info["data_file_id"]})
    return included
