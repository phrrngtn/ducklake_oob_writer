"""Content-hash incorporation log + lake_as_known_at (knowledge-time projection).

register_parquet records each incorporation (content hash + both clocks) in the
ancillary `oob_incorporation` log. lake_as_known_at projects a catalog over the same
Parquet containing only the files incorporated up to a cutoff, in transaction-time
order — proven by a native-reader round-trip.

Run: uv run --group dev pytest tests/test_incorporation.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine, select

import ducklake_oob_writer as dl
from ducklake_oob_writer.incorporation import INCORPORATION_LOG


def _markers(catalog_path, data, *, at=None):
    clause = f" AT (TIMESTAMP => TIMESTAMP '{at}')" if at else ""
    with dl.attach_lake(f"sqlite:{catalog_path}", data) as c:
        return {m: n for m, n in c.execute(
            f"SELECT m, count(*) FROM lake.facts{clause} GROUP BY 1 ORDER BY 1").fetchall()}


def test_incorporation_log_and_lake_as_known_at(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    cat = os.path.join(str(tmp_path), "cat.sqlite")
    os.makedirs(os.path.join(data, "main", "facts"), exist_ok=True)
    # 3 files; B is a backfill (earlier transaction-time, later incorporation)
    plan = [("A", 0, 10, dt.datetime(2026, 6, 10)),
            ("B", 10, 15, dt.datetime(2026, 6, 5)),
            ("C", 15, 23, dt.datetime(2026, 6, 15))]

    dw = duckdb.connect()
    eng = create_engine(f"sqlite:///{cat}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "facts", [("m", "varchar"), ("id", "int64")],
                   snapshot_time=dt.datetime(2026, 6, 1))
    for m, lo, hi, tt in plan:
        f = os.path.join(data, "main", "facts", f"{m}.parquet")
        dw.execute(f"COPY (SELECT '{m}' AS m, range AS id FROM range({lo},{hi})) "
                   f"TO '{f}' (FORMAT PARQUET)")
        w.register_parquet("facts", f, snapshot_time=tt, source_uri=f"file://{f}")
    dw.close()

    # the log: one monotonic row per incorporation, both clocks, sha256 content hashes
    with eng.connect() as c:
        rows = c.execute(
            select(INCORPORATION_LOG.c.incorporation_seq, INCORPORATION_LOG.c.content_hash)
            .order_by(INCORPORATION_LOG.c.incorporation_seq)).fetchall()
    assert [r[0] for r in rows] == [0, 1, 2]            # monotonic incorporation cursor
    assert all(len(r[1]) == 64 for r in rows)           # sha256 hex digests
    eng.dispose()

    def project(name, **cut):
        tgt = os.path.join(str(tmp_path), f"{name}.sqlite")
        se, te = create_engine(f"sqlite:///{cat}"), create_engine(f"sqlite:///{tgt}")
        dl.lake_as_known_at(se, te, **cut)
        se.dispose(); te.dispose()
        return tgt

    # as we knew it right after incorporating A: A only
    assert _markers(project("k0", seq=0), data) == {"A": 10}
    # after A and the B backfill: both, and AT(06-07) inside the projection sees only B
    k1 = project("k1", seq=1)
    assert _markers(k1, data) == {"A": 10, "B": 5}
    assert _markers(k1, data, at="2026-06-07") == {"B": 5}
    # after all three
    assert _markers(project("k2", seq=2), data) == {"A": 10, "B": 5, "C": 8}
