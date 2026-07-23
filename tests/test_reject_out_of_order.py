"""Tests for the reject_out_of_order writer mode (per-table transaction-time monotonicity).

With reject_out_of_order=True, a data/inline write whose snapshot_time precedes an existing
snapshot for the SAME table is refused (ValueError) instead of accepted + canonicalized — so
snapshot_id stays a stable, monotonic cursor for downstream per-table TTST subscribers. The
guard is per-table: independently-tailed tables advance on their own source clocks.

Run: uv run --group dev pytest tests/test_reject_out_of_order.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine, inspect, text

import ducklake_oob_writer as dl


def _writer(cat, data, reject):
    eng = create_engine(f"sqlite:///{cat}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA, reject_out_of_order=reject)
    w.init_catalog(data_path=data)
    return eng, w


def _parquet(data, table, fn, marker, lo, hi):
    os.makedirs(os.path.join(data, "main", table), exist_ok=True)
    path = os.path.join(data, "main", table, fn)
    d = duckdb.connect()
    d.execute(f"COPY (SELECT '{marker}' AS m, range AS id FROM range({lo},{hi})) "
              f"TO '{path}' (FORMAT PARQUET)")
    d.close()
    return path


def test_in_order_ok_then_out_of_order_raises(tmp_path):
    data, cat = os.path.join(str(tmp_path), "data"), os.path.join(str(tmp_path), "cat.sqlite")
    eng, w = _writer(cat, data, reject=True)
    w.create_table("main", "facts", [("m", "varchar"), ("id", "int64")],
                   snapshot_time=dt.datetime(2026, 6, 1))
    p10 = _parquet(data, "facts", "june10.parquet", "june10", 0, 10)
    p05 = _parquet(data, "facts", "june05.parquet", "june05", 100, 105)

    w.register_parquet("facts", p10, snapshot_time=dt.datetime(2026, 6, 10, 12))     # in order: ok
    with pytest.raises(ValueError, match="out-of-order"):                            # backfill: refused
        w.register_parquet("facts", p05, snapshot_time=dt.datetime(2026, 6, 5, 12))
    eng.dispose()


def test_guard_is_per_table_not_global(tmp_path):
    data, cat = os.path.join(str(tmp_path), "data"), os.path.join(str(tmp_path), "cat.sqlite")
    eng, w = _writer(cat, data, reject=True)
    for t in ("a", "b"):
        w.create_table("main", t, [("m", "varchar"), ("id", "int64")],
                       snapshot_time=dt.datetime(2026, 6, 1))
    w.register_parquet("a", _parquet(data, "a", "a.parquet", "a", 0, 5),
                       snapshot_time=dt.datetime(2026, 6, 20, 12))     # A far ahead
    # B at June 5 is earlier than A's frontier — but fine, different table
    w.register_parquet("b", _parquet(data, "b", "b.parquet", "b", 0, 5),
                       snapshot_time=dt.datetime(2026, 6, 5, 12))
    # B going backwards vs its OWN frontier is refused
    with pytest.raises(ValueError, match="out-of-order"):
        w.register_parquet("b", _parquet(data, "b", "b2.parquet", "b2", 5, 9),
                           snapshot_time=dt.datetime(2026, 6, 3, 12))
    eng.dispose()


def test_inline_rows_path_is_guarded(tmp_path):
    """The CDC/CT tail path (inline MVCC) — the one lakereplica's TTST fan-out uses."""
    data, cat = os.path.join(str(tmp_path), "data"), os.path.join(str(tmp_path), "cat.sqlite")
    eng, w = _writer(cat, data, reject=True)
    w.create_table("main", "t", [("id", "int64"), ("v", "varchar")],
                   snapshot_time=dt.datetime(2026, 6, 1))
    w.inline_rows("t", [{"id": 1, "v": "a"}], snapshot_time=dt.datetime(2026, 6, 10, 12))
    with pytest.raises(ValueError, match="out-of-order"):
        w.inline_rows("t", [{"id": 2, "v": "b"}], snapshot_time=dt.datetime(2026, 6, 5, 12))
    eng.dispose()


def test_equal_timestamp_is_allowed(tmp_path):
    """Monotonicity is non-strict — a tie (same snapshot_time) is fine, not out of order."""
    data, cat = os.path.join(str(tmp_path), "data"), os.path.join(str(tmp_path), "cat.sqlite")
    eng, w = _writer(cat, data, reject=True)
    w.create_table("main", "t", [("id", "int64"), ("v", "varchar")],
                   snapshot_time=dt.datetime(2026, 6, 1))
    ts = dt.datetime(2026, 6, 10, 12)
    w.inline_rows("t", [{"id": 1, "v": "a"}], snapshot_time=ts)
    w.inline_rows("t", [{"id": 2, "v": "b"}], snapshot_time=ts)          # equal: allowed
    eng.dispose()


def test_ancillary_index_only_exists_in_reject_mode(tmp_path):
    """Attribution is a structured ancillary table (oob_snapshot_table), not changes_made
    parsing. Default mode never creates it (byte-unchanged for existing consumers); reject mode
    creates + populates it. The ducklake extension ignores it (not a ducklake_* table)."""
    # default mode: no ancillary table
    d0, c0 = os.path.join(str(tmp_path), "d0"), os.path.join(str(tmp_path), "c0.sqlite")
    eng0, w0 = _writer(c0, d0, reject=False)
    w0.create_table("main", "t", [("id", "int64"), ("v", "varchar")],
                    snapshot_time=dt.datetime(2026, 6, 1))
    w0.inline_rows("t", [{"id": 1, "v": "a"}], snapshot_time=dt.datetime(2026, 6, 10, 12))
    assert not inspect(eng0).has_table("oob_snapshot_table")
    eng0.dispose()

    # reject mode: table created + populated (one row per temporal snapshot)
    d1, c1 = os.path.join(str(tmp_path), "d1"), os.path.join(str(tmp_path), "c1.sqlite")
    eng1, w1 = _writer(c1, d1, reject=True)
    w1.create_table("main", "t", [("id", "int64"), ("v", "varchar")],
                    snapshot_time=dt.datetime(2026, 6, 1))
    w1.inline_rows("t", [{"id": 1, "v": "a"}], snapshot_time=dt.datetime(2026, 6, 10, 12))
    assert inspect(eng1).has_table("oob_snapshot_table")
    with eng1.connect() as c:
        assert c.execute(text("SELECT count(*) FROM oob_snapshot_table")).scalar() >= 1
    eng1.dispose()


def test_default_still_accepts_and_canonicalizes_out_of_order(tmp_path):
    """Backward-compat: the default writer accepts an OOO backfill and canonicalizes it."""
    data, cat = os.path.join(str(tmp_path), "data"), os.path.join(str(tmp_path), "cat.sqlite")
    eng, w = _writer(cat, data, reject=False)
    w.create_table("main", "facts", [("m", "varchar"), ("id", "int64")],
                   snapshot_time=dt.datetime(2026, 6, 1))
    w.register_parquet("facts", _parquet(data, "facts", "j10.parquet", "june10", 0, 10),
                       snapshot_time=dt.datetime(2026, 6, 10, 12))
    w.register_parquet("facts", _parquet(data, "facts", "j05.parquet", "june05", 100, 105),
                       snapshot_time=dt.datetime(2026, 6, 5, 12))       # OOO backfill: accepted
    eng.dispose()
    with dl.attach_lake(f"sqlite:{cat}", data) as c:                    # canonicalized -> correct AS-OF
        at7 = sorted(x[0] for x in c.execute(
            "SELECT DISTINCT m FROM lake.facts AT (TIMESTAMP => TIMESTAMP '2026-06-07')").fetchall())
    assert at7 == ["june05"]
