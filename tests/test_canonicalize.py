"""Tests for transaction-time canonicalization.

DuckLake orders state by the surrogate snapshot_id, so out-of-order (backfilled)
registrations break AT(TIMESTAMP) time-travel. `recanonicalize` rebuilds the
catalog in transaction-time order and fixes it — deterministically, touching no
Parquet (it reuses the stats already in the catalog).

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
    """commit_order: list of marker names in the order they are *committed*."""
    os.makedirs(os.path.join(data, "main", "facts"), exist_ok=True)
    src = {"june05": ("june05.parquet", 100, 105, dt.datetime(2026, 6, 5, 12)),
           "june10": ("june10.parquet", 0, 10, dt.datetime(2026, 6, 10, 12))}
    dw = duckdb.connect()
    for m in src:
        fn, lo, hi, _ = src[m]
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


def test_out_of_order_breaks_then_recanonicalize_fixes(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    src_cat = os.path.join(str(tmp_path), "broken.sqlite")
    # backfill: commit the later-dated file first, the older-dated file second
    _build(src_cat, data, ["june10", "june05"])

    # broken: AT 06-07 leaks the future june10; AT 06-12 drops the june05 backfill
    assert _markers_at(src_cat, data, "2026-06-07") == ["june05", "june10"]
    assert _markers_at(src_cat, data, "2026-06-12") == ["june10"]

    # recanonicalize into a fresh catalog over the SAME data files
    canon_cat = os.path.join(str(tmp_path), "canon.sqlite")
    src_eng = create_engine(f"sqlite:///{src_cat}")
    tgt_eng = create_engine(f"sqlite:///{canon_cat}")
    dl.recanonicalize(src_eng, tgt_eng)
    src_eng.dispose(); tgt_eng.dispose()

    # fixed: as-of June 7 sees only the June 5 fact; as-of June 12 sees both
    assert _markers_at(canon_cat, data, "2026-06-07") == ["june05"]
    assert _markers_at(canon_cat, data, "2026-06-12") == ["june05", "june10"]


def test_recanonicalize_is_idempotent_on_in_order(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    src_cat = os.path.join(str(tmp_path), "ordered.sqlite")
    _build(src_cat, data, ["june05", "june10"])           # already in source-time order
    before = _markers_at(src_cat, data, "2026-06-07")

    canon_cat = os.path.join(str(tmp_path), "canon.sqlite")
    src_eng = create_engine(f"sqlite:///{src_cat}")
    tgt_eng = create_engine(f"sqlite:///{canon_cat}")
    dl.recanonicalize(src_eng, tgt_eng)
    src_eng.dispose(); tgt_eng.dispose()

    assert before == ["june05"]
    assert _markers_at(canon_cat, data, "2026-06-07") == ["june05"]
    assert _markers_at(canon_cat, data, "2026-06-12") == ["june05", "june10"]
