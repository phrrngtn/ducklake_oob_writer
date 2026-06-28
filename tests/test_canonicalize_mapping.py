"""recanonicalize must preserve register_virtual (hive) mappings.

The in-place renumber only touches snapshot ids and the references to them; the
column/name mappings carry no snapshot id, so they survive automatically. Proven
here by a native-reader round-trip after canonicalizing an out-of-order lake whose
columns are path-virtualized.

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
    cat = os.path.join(str(tmp_path), "cat.sqlite")

    dw = duckdb.connect()
    rels = []
    for c, lo, hi in [("sales", 0, 3000), ("returns", 3000, 4000)]:
        d = os.path.join(data, "main", "events", f"category={c}")
        os.makedirs(d, exist_ok=True)
        f = os.path.join(d, "part-0.parquet")
        dw.execute(f"COPY (SELECT 'sensor_'||(range%3) AS sensor, (range*1.0)::DOUBLE AS value "
                   f"FROM range({lo},{hi})) TO '{f}' (FORMAT PARQUET)")
        rels.append((f, f"category={c}/part-0.parquet"))
    dw.close()

    eng = create_engine(f"sqlite:///{cat}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "events",
                   [("category", "varchar"), ("sensor", "varchar"), ("value", "float64")])
    # registered OUT of source-time order (returns is a backfill) AND path-virtual
    w.register_virtual("events", rels[0][0], rel_path=rels[0][1], snapshot_time=dt.datetime(2026, 6, 10))
    w.register_virtual("events", rels[1][0], rel_path=rels[1][1], snapshot_time=dt.datetime(2026, 6, 5))
    eng.dispose()                     # register_data_file already canonicalized in-transaction

    with dl.attach_lake(f"sqlite:{cat}", data) as c:
        # the path-virtual column still materializes + prunes after canonicalization
        assert c.execute("SELECT category, count(*) FROM lake.events GROUP BY 1 ORDER BY 1"
                         ).fetchall() == [("returns", 1000), ("sales", 3000)]
        assert c.execute("SELECT count(*) FROM lake.events WHERE category='sales'"
                         ).fetchone()[0] == 3000
        plan = c.execute("EXPLAIN ANALYZE SELECT count(*) FROM lake.events "
                         "WHERE category='sales'").fetchall()[-1][-1]
        assert "Total Files Read: 1" in plan
        # transaction-time order is now correct (the 06-05 backfill precedes 06-10)
        assert c.execute("SELECT category FROM lake.events "
                         "AT (TIMESTAMP => TIMESTAMP '2026-06-07') GROUP BY 1").fetchall() == [("returns",)]
