"""recanonicalize must preserve register_virtual (hive) mappings.

Canonicalizing rewrites every snapshot_id and re-points surrogate ids; the
column/name mappings have to be carried (re-pointed at the rebuilt tables' column
ids) or the path-virtualized columns silently break. This is the seam between the
dimension-encoding axis and the temporal-order axis — proven closed here by a
native-reader round-trip after a canonicalize.

Run: uv run --group dev pytest tests/test_canonicalize_mapping.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl


def test_recanonicalize_preserves_virtual_mappings(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    src = os.path.join(str(tmp_path), "src.sqlite")

    dw = duckdb.connect()
    rels = []
    for cat, lo, hi in [("sales", 0, 3000), ("returns", 3000, 4000)]:
        d = os.path.join(data, "main", "events", f"category={cat}")
        os.makedirs(d, exist_ok=True)
        f = os.path.join(d, "part-0.parquet")
        dw.execute(f"COPY (SELECT 'sensor_'||(range%3) AS sensor, (range*1.0)::DOUBLE AS value "
                   f"FROM range({lo},{hi})) TO '{f}' (FORMAT PARQUET)")
        rels.append((f, f"category={cat}/part-0.parquet"))
    dw.close()

    eng = create_engine(f"sqlite:///{src}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "events",
                   [("category", "varchar"), ("sensor", "varchar"), ("value", "float64")])
    # registered OUT of source-time order (returns is a backfill) AND path-virtual
    w.register_virtual("events", rels[0][0], rel_path=rels[0][1], snapshot_time=dt.datetime(2026, 6, 10))
    w.register_virtual("events", rels[1][0], rel_path=rels[1][1], snapshot_time=dt.datetime(2026, 6, 5))
    eng.dispose()

    canon = os.path.join(str(tmp_path), "canon.sqlite")
    se = create_engine(f"sqlite:///{src}")
    te = create_engine(f"sqlite:///{canon}")
    dl.recanonicalize(se, te)
    se.dispose(); te.dispose()

    with dl.attach_lake(f"sqlite:{canon}", data) as c:
        # the path-virtual column still materializes + prunes after canonicalization
        assert c.execute("SELECT category, count(*) FROM lake.events GROUP BY 1 ORDER BY 1"
                         ).fetchall() == [("returns", 1000), ("sales", 3000)]
        assert c.execute("SELECT count(*) FROM lake.events WHERE category='sales'"
                         ).fetchone()[0] == 3000
        plan = c.execute("EXPLAIN ANALYZE SELECT count(*) FROM lake.events "
                         "WHERE category='sales'").fetchall()[-1][-1]
        assert "Total Files Read: 1" in plan
        # and transaction-time order is now correct (the backfill at 06-05 precedes 06-10)
        assert c.execute("SELECT category FROM lake.events "
                         "AT (TIMESTAMP => TIMESTAMP '2026-06-07') GROUP BY 1").fetchall() == [("returns",)]
