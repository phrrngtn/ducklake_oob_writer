"""Full transaction-time history from CDC via inline MVCC (HistoryReplica).

Unlike the net/current-state Replica, this keeps EVERY intermediate version: each source
commit is one snapshot, inserts/updates/deletes become inline MVCC (begin/end_snapshot).
AT(TIMESTAMP) replays to after any commit, and a key's full version list is recoverable.

Run: uv run --group dev pytest tests/test_history_replica.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
from sqlalchemy import create_engine, text

import ducklake_oob_writer as dl


def test_history_replica_keeps_intermediate_versions(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    cat = os.path.join(str(tmp_path), "cat.sqlite")
    eng = create_engine(f"sqlite:///{cat}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "t", [("id", "int64"), ("name", "varchar")])
    h = dl.HistoryReplica(w, "t", "id")

    T1 = dt.datetime(2026, 6, 29, 10)
    T2 = dt.datetime(2026, 6, 29, 11)
    T3 = dt.datetime(2026, 6, 29, 12)

    # commit 1: insert 3
    h.apply_commit([{"op": "I", "key": 1, "row": {"id": 1, "name": "a"}},
                    {"op": "I", "key": 2, "row": {"id": 2, "name": "b"}},
                    {"op": "I", "key": 3, "row": {"id": 3, "name": "c"}}], snapshot_time=T1)
    # commit 2: update id=2 -> b2, delete id=3, insert id=4
    h.apply_commit([{"op": "U", "key": 2, "row": {"id": 2, "name": "b2"}},
                    {"op": "D", "key": 3},
                    {"op": "I", "key": 4, "row": {"id": 4, "name": "d"}}], snapshot_time=T2)
    # commit 3: update id=2 again -> b3 (the b2 is an INTERMEDIATE a net feed would drop)
    h.apply_commit([{"op": "U", "key": 2, "row": {"id": 2, "name": "b3"}}], snapshot_time=T3)
    eng.dispose()

    with dl.attach_lake(f"sqlite:{cat}", data) as c:
        def state(at=None):
            clause = f" AT (TIMESTAMP => TIMESTAMP '{at}')" if at else ""
            return dict(c.execute(f"SELECT id, name FROM lake.t{clause} ORDER BY id").fetchall())

        # current state
        assert state() == {1: "a", 2: "b3", 4: "d"}            # id 3 deleted, id 2 = latest
        # full transaction-time history — replay to after any commit
        assert state("2026-06-29 10:30") == {1: "a", 2: "b", 3: "c"}     # after commit 1
        assert state("2026-06-29 11:30") == {1: "a", 2: "b2", 4: "d"}    # after commit 2 (b2!)
        assert state("2026-06-29 12:30") == {1: "a", 2: "b3", 4: "d"}    # after commit 3

    # the INTERMEDIATE version b2 is retained — every version of id=2 is present
    import duckdb
    import sqlite3
    name = sqlite3.connect(cat).execute(
        "SELECT table_name FROM ducklake_inlined_data_tables").fetchone()[0]
    versions = [r[0] for r in sqlite3.connect(cat).execute(
        f'SELECT name FROM {name} WHERE id = 2 ORDER BY begin_snapshot').fetchall()]
    assert versions == ["b", "b2", "b3"]                        # full version lineage of id=2
