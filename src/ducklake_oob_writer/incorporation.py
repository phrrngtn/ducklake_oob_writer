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
from sqlalchemy import (BigInteger, Column, DateTime, MetaData, Table, Text, and_, func,
                        select)

from ducklake_oob_writer.catalog import DUCKLAKE_METADATA, create_catalog

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

    M = DUCKLAKE_METADATA.tables
    meta_t, tbl_t, sch_t, col_t = (M["ducklake_metadata"], M["ducklake_table"],
                                   M["ducklake_schema"], M["ducklake_column"])
    pcol_t, pinfo_t = M["ducklake_partition_column"], M["ducklake_partition_info"]
    df_t, snap_t, fcs_t = (M["ducklake_data_file"], M["ducklake_snapshot"],
                           M["ducklake_file_column_stats"])
    cmap_t, nmap_t = M["ducklake_column_mapping"], M["ducklake_name_mapping"]

    with source_engine.connect() as src:
        data_path = src.execute(select(meta_t.c.value)
                                .where(meta_t.c.key == "data_path")).scalar()
        included = {r[0] for r in src.execute(select(log.c.data_file_id).where(cut))}

        # schema (current tables + columns) and any partition specs
        tables = src.execute(
            select(tbl_t.c.table_id, sch_t.c.schema_name, tbl_t.c.table_name)
            .select_from(tbl_t.join(sch_t, tbl_t.c.schema_id == sch_t.c.schema_id))
            .where(and_(tbl_t.c.end_snapshot.is_(None), sch_t.c.end_snapshot.is_(None)))
            .order_by(tbl_t.c.table_id)).fetchall()
        cols, partcols = {}, {}
        for tid, sname, tname in tables:
            colrows = src.execute(
                select(col_t.c.column_id, col_t.c.column_name, col_t.c.column_type)
                .where(and_(col_t.c.table_id == tid, col_t.c.end_snapshot.is_(None)))
                .order_by(col_t.c.column_order)).fetchall()
            cols[tid] = (sname, tname, [(n, ty) for _, n, ty in colrows])
            id2name = {cid: n for cid, n, _ in colrows}
            pc = src.execute(
                select(pcol_t.c.column_id)
                .select_from(pcol_t.join(pinfo_t, pcol_t.c.partition_id == pinfo_t.c.partition_id))
                .where(and_(pcol_t.c.table_id == tid, pinfo_t.c.end_snapshot.is_(None)))
                .order_by(pcol_t.c.partition_key_index)).fetchall()
            partcols[tid] = [id2name[cid] for (cid,) in pc if cid in id2name]

        # the included files, with stats + transaction-time + mapping
        files = []
        file_rows = src.execute(
            select(df_t.c.data_file_id, df_t.c.table_id, df_t.c.path, df_t.c.record_count,
                   df_t.c.file_size_bytes, df_t.c.footer_size, snap_t.c.snapshot_time,
                   df_t.c.mapping_id)
            .select_from(df_t.join(snap_t, df_t.c.begin_snapshot == snap_t.c.snapshot_id))
            .where(df_t.c.end_snapshot.is_(None))).fetchall()
        for fid, tid, path, rc, fsz, foot, st, mid in file_rows:
            if fid not in included:
                continue
            stat_rows = src.execute(
                select(col_t.c.column_name, fcs_t.c.min_value, fcs_t.c.max_value,
                       fcs_t.c.value_count, fcs_t.c.null_count, fcs_t.c.column_size_bytes,
                       fcs_t.c.contains_nan)
                .select_from(fcs_t.join(col_t, fcs_t.c.column_id == col_t.c.column_id))
                .where(and_(fcs_t.c.data_file_id == fid,
                            col_t.c.end_snapshot.is_(None)))).fetchall()
            stats = {n: {"min_value": mn, "max_value": mx, "value_count": vc,
                         "null_count": nc, "column_size_bytes": csz, "contains_nan": cn}
                     for n, mn, mx, vc, nc, csz, cn in stat_rows}
            files.append({"table_id": tid, "path": path, "record_count": rc,
                          "file_size_bytes": fsz, "footer_size": foot,
                          "transaction_time": _parse_ts(st), "stats": stats or None, "mapping_id": mid})

        logger.info(
            "lake_as_known_at({cut}): projecting {n} file(s) over the shared Parquet",
            cut=(f"seq<={seq}" if seq is not None else f"time<={incorporation_time}"),
            n=len(included))

        mappings = {}
        for mid, mtid, mtype in src.execute(
                select(cmap_t.c.mapping_id, cmap_t.c.table_id, cmap_t.c.type)).fetchall():
            nm = src.execute(
                select(nmap_t.c.source_name, col_t.c.column_name, nmap_t.c.is_partition)
                .select_from(nmap_t.join(col_t, nmap_t.c.target_field_id == col_t.c.column_id))
                .where(and_(nmap_t.c.mapping_id == mid, col_t.c.end_snapshot.is_(None)))
                .order_by(nmap_t.c.column_id)).fetchall()
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
            tc.execute(cmap_t.insert().values(mapping_id=new_mid, table_id=tgt_tid, type=m["type"]))
            for idx, (sn, cn, isp) in enumerate(m["cols"]):
                tc.execute(nmap_t.insert().values(
                    mapping_id=new_mid, column_id=idx, source_name=sn,
                    target_field_id=field_of[tgt_tid][cn], parent_column=None, is_partition=isp))

    name_of = {tid: cols[tid] for tid in cols}
    for f in sorted(files, key=lambda f: (f["transaction_time"], f["path"])):
        sname, tname, _ = name_of[f["table_id"]]
        info = w.register_data_file(
            tname, path=f["path"], record_count=f["record_count"],
            file_size_bytes=f["file_size_bytes"], footer_size=f["footer_size"],
            schema_name=sname, column_stats=f["stats"], snapshot_time=f["transaction_time"])
        if f["mapping_id"] is not None and f["mapping_id"] in old_to_new_mid:
            with target_engine.begin() as tc:
                tc.execute(df_t.update().where(df_t.c.data_file_id == info["data_file_id"])
                           .values(mapping_id=old_to_new_mid[f["mapping_id"]]))
    return included
