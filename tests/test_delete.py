"""OOB file-resident deletes via DuckLake position delete-files (merge-on-read).

delete_rows / delete_where write a (file_path, pos) delete-file and register it in
ducklake_delete_file — the data file is untouched. Proven by a native-reader round-trip.
Out-of-order (backfilled) deletes are refused.

Run: uv run --group dev pytest tests/test_delete.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl


def _mk(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    cat = os.path.join(str(tmp_path), "cat.sqlite")
    os.makedirs(os.path.join(data, "main", "t"))
    f = os.path.join(data, "main", "t", "data.parquet")
    duckdb.connect().execute(
        f"COPY (SELECT range AS id, 'r' || range AS name FROM range(0,10)) "
        f"TO '{f}' (FORMAT PARQUET)")
    eng = create_engine(f"sqlite:///{cat}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "t", [("id", "int64"), ("name", "varchar")])
    w.register_parquet("t", f, rel_path="data.parquet", snapshot_time=dt.datetime(2026, 6, 10))
    return data, cat, eng, w


def test_delete_where_roundtrip(tmp_path):
    data, cat, eng, w = _mk(tmp_path)
    res = w.delete_where("t", "data.parquet", "id IN (2,5,7)",
                         snapshot_time=dt.datetime(2026, 6, 11))
    assert res["deleted"] == 3
    eng.dispose()

    with dl.attach_lake(f"sqlite:{cat}", data) as c:
        # merge-on-read: the 3 positions are gone, the data file untouched
        assert c.execute("SELECT count(*) FROM lake.t").fetchone()[0] == 7
        assert [r[0] for r in c.execute("SELECT id FROM lake.t ORDER BY id").fetchall()] == [0, 1, 3, 4, 6, 8, 9]
        # time-travel: before the delete (June 10) all 10 rows are present
        assert c.execute("SELECT count(*) FROM lake.t "
                         "AT (TIMESTAMP => TIMESTAMP '2026-06-10 12:00')").fetchone()[0] == 10


def test_update_is_delete_plus_register(tmp_path):
    data, cat, eng, w = _mk(tmp_path)
    # "update" id=5: delete its position, then register a replacement file with the new row
    w.delete_where("t", "data.parquet", "id = 5", snapshot_time=dt.datetime(2026, 6, 11))
    upd = os.path.join(data, "main", "t", "upd.parquet")
    duckdb.connect().execute(f"COPY (SELECT 5 AS id, 'FIVE' AS name) TO '{upd}' (FORMAT PARQUET)")
    w.register_parquet("t", upd, rel_path="upd.parquet", snapshot_time=dt.datetime(2026, 6, 12))
    eng.dispose()
    with dl.attach_lake(f"sqlite:{cat}", data) as c:
        assert c.execute("SELECT name FROM lake.t WHERE id = 5").fetchone()[0] == "FIVE"
        assert c.execute("SELECT count(*) FROM lake.t").fetchone()[0] == 10


def test_out_of_order_delete_is_refused(tmp_path):
    data, cat, eng, w = _mk(tmp_path)              # insert dated June 10
    with pytest.raises(ValueError, match="out-of-order delete|can't do that"):
        w.delete_where("t", "data.parquet", "id = 1", snapshot_time=dt.datetime(2026, 6, 5))
    eng.dispose()
