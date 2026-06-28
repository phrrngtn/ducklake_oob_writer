"""Round-trip test for register_virtual (relocate / hive-path style).

A Parquet file laid out at a `key=value` path, registered with register_virtual,
must read back through the native ducklake engine with the path columns
*materialized* (they are not in the file) and *pruned* on — no Parquet rewrite.

Run: uv run --group dev pytest tests/test_virtual.py -v
"""
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl


def _build(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    catalog = f"sqlite:{tmp_path}/cat.sqlite"
    # files hold ONLY (sensor, value); `category` lives in the hive path
    dw = duckdb.connect()
    files = []
    for cat, lo, hi in [("sales", 0, 3000), ("returns", 3000, 4000)]:
        d = os.path.join(data, "main", "events", f"category={cat}")
        os.makedirs(d, exist_ok=True)
        f = os.path.join(d, "part-0.parquet")
        dw.execute(f"COPY (SELECT 'sensor_'||(range%3) AS sensor, (range*1.0)::DOUBLE AS value "
                   f"FROM range({lo},{hi})) TO '{f}' (FORMAT PARQUET)")
        files.append((f, f"category={cat}/part-0.parquet"))
    dw.close()

    eng = create_engine(f"sqlite:///{tmp_path}/cat.sqlite")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    # table declares `category` even though no file contains it
    w.create_table("main", "events",
                   [("category", "varchar"), ("sensor", "varchar"), ("value", "float64")])
    for fs_path, rel in files:
        r = w.register_virtual("events", fs_path, rel_path=rel)
        assert r["virtual"] == {"category": fs_path.split("category=")[1].split("/")[0]}
    eng.dispose()
    return catalog, data


def test_register_virtual_materializes_and_prunes(tmp_path):
    catalog, data = _build(tmp_path)
    with dl.attach_lake(catalog, data) as c:
        # the path column is materialized though it is in no file
        assert c.execute("SELECT category, count(*) FROM lake.events GROUP BY 1 ORDER BY 1"
                         ).fetchall() == [("returns", 1000), ("sales", 3000)]
        # the real (in-file) columns still read correctly
        assert c.execute("SELECT count(DISTINCT sensor) FROM lake.events").fetchone()[0] == 3
        # predicate on the virtual column is correct AND prunes to one file
        assert c.execute("SELECT count(*) FROM lake.events WHERE category='sales'"
                         ).fetchone()[0] == 3000
        plan = c.execute("EXPLAIN ANALYZE SELECT count(*) FROM lake.events "
                         "WHERE category='sales'").fetchall()[-1][-1]
        assert "Total Files Read: 1" in plan
