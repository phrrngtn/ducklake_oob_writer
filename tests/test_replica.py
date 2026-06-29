"""Current-state CDC/CT replica — net-change merge into a DuckLake table.

A source is polled for net changes (CT, or CDC treated as CT): upserts are the full
current rows for inserted/updated keys, deletes are gone keys. Replica.apply() merges
them — superseding old rows via position delete-files and registering new ones — so the
DuckLake table mirrors the source's *current* state, with full transaction-time history.
Proven by native-reader round-trips on the current state and on AT(TIMESTAMP).

Run: uv run --group dev pytest tests/test_replica.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
from sqlalchemy import create_engine

import ducklake_oob_writer as dl


def test_current_state_replica_merge_and_history(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    cat = os.path.join(str(tmp_path), "cat.sqlite")
    eng = create_engine(f"sqlite:///{cat}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "customer",
                   [("id", "int64"), ("name", "varchar"), ("region", "varchar")])
    rep = dl.Replica(w, "customer", "id")

    T1 = dt.datetime(2026, 6, 10)
    T2 = dt.datetime(2026, 6, 11)

    # poll 1 (initial load): three rows
    rep.apply(upserts=[{"id": 1, "name": "a", "region": "X"},
                       {"id": 2, "name": "b", "region": "X"},
                       {"id": 3, "name": "c", "region": "Y"}], snapshot_time=T1)
    # poll 2 (net changes since the watermark): update id=2, delete id=3
    res = rep.apply(upserts=[{"id": 2, "name": "b2", "region": "Z"}],
                    deletes=[3], snapshot_time=T2)
    # 1 new row; 2 old rows superseded (the pre-update id=2 AND the deleted id=3)
    assert res == {"upserted": 1, "deleted": 2}
    eng.dispose()

    with dl.attach_lake(f"sqlite:{cat}", data) as c:
        # current state mirrors the source: id 3 gone, id 2 updated
        assert dict(c.execute("SELECT id, name FROM lake.customer ORDER BY id").fetchall()) \
            == {1: "a", 2: "b2"}
        assert c.execute("SELECT region FROM lake.customer WHERE id = 2").fetchone()[0] == "Z"
        assert c.execute("SELECT count(*) FROM lake.customer").fetchone()[0] == 2

        # transaction-time history: as of poll 1, all three originals (id 2 = 'b'/'X')
        asof1 = dict(c.execute("SELECT id, name FROM lake.customer "
                               "AT (TIMESTAMP => TIMESTAMP '2026-06-10 12:00') ORDER BY id").fetchall())
        assert asof1 == {1: "a", 2: "b", 3: "c"}
