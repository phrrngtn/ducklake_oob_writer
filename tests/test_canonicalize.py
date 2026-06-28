"""Tests for in-place transaction-time canonicalization.

DuckLake orders state by the surrogate snapshot_id, so out-of-order (backfilled)
registrations break AT(TIMESTAMP) time-travel. `recanonicalize` renumbers the
snapshots in place so snapshot_id order matches snapshot_time order — idempotent,
one transaction, set-based.

Run: uv run --group dev pytest tests/test_canonicalize.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl


def _markers_at(catalog_path, data, ts):
    with dl.attach_lake(f"sqlite:{catalog_path}", data) as c:
        return sorted(x[0] for x in c.execute(
            f"SELECT DISTINCT m FROM lake.facts AT (TIMESTAMP => TIMESTAMP '{ts}')"
        ).fetchall())


def _build(catalog_path, data, commit_order):
    """commit_order: marker names in the order they are *committed* (snapshot_id)."""
    os.makedirs(os.path.join(data, "main", "facts"), exist_ok=True)
    src = {"june05": ("june05.parquet", 100, 105, dt.datetime(2026, 6, 5, 12)),
           "june10": ("june10.parquet", 0, 10, dt.datetime(2026, 6, 10, 12))}
    dw = duckdb.connect()
    for m, (fn, lo, hi, _) in src.items():
        dw.execute(f"COPY (SELECT '{m}' AS m, range AS id FROM range({lo},{hi})) "
                   f"TO '{os.path.join(data, 'main', 'facts', fn)}' (FORMAT PARQUET)")
    dw.close()
    eng = create_engine(f"sqlite:///{catalog_path}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "facts", [("m", "varchar"), ("id", "int64")],
                   snapshot_time=dt.datetime(2026, 6, 1))
    for m in commit_order:
        fn, _, _, t = src[m]
        w.register_parquet("facts", os.path.join(data, "main", "facts", fn), snapshot_time=t)
    eng.dispose()


def test_out_of_order_arrival_is_auto_canonicalized(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    cat = os.path.join(str(tmp_path), "cat.sqlite")
    # Backfill: commit the later-dated file first, the older-dated file second.
    # register_data_file renumbers in the SAME transaction, so a broken state is
    # never observable — no explicit recanonicalize call, no "assert broken" phase.
    _build(cat, data, ["june10", "june05"])

    # as-of June 7 sees only the June 5 fact; as-of June 12 sees both
    assert _markers_at(cat, data, "2026-06-07") == ["june05"]
    assert _markers_at(cat, data, "2026-06-12") == ["june05", "june10"]


def test_recanonicalize_is_noop_when_in_order(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    cat = os.path.join(str(tmp_path), "cat.sqlite")
    _build(cat, data, ["june05", "june10"])            # already in source-time order
    before = _markers_at(cat, data, "2026-06-07")

    eng = create_engine(f"sqlite:///{cat}")
    dl.recanonicalize(eng)                             # idempotent no-op
    dl.recanonicalize(eng)                             # and again
    eng.dispose()

    assert before == ["june05"]
    assert _markers_at(cat, data, "2026-06-07") == ["june05"]
    assert _markers_at(cat, data, "2026-06-12") == ["june05", "june10"]
