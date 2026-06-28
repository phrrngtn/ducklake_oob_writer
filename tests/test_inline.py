"""OOB-written DuckLake inlined data: no Parquet, no DuckDB on the write side.

inline_rows writes rows straight into the catalog in DuckLake's native inlining format
(ducklake_inlined_data_<tid>_<sv>, MVCC row-versioning). Proven by native-reader
round-trips: the engine unions inlined rows with Parquet, time-travels them, parses
decimal/timestamp text back, and ducklake_flush_inlined_data squishes them to Parquet.
Out-of-order inlined backfills self-canonicalize (incl. the inlined begin/end_snapshot).

Run: uv run --group dev pytest tests/test_inline.py -v
"""
import datetime as dt
import decimal
import glob
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl


def _mk(tmp_path, cols):
    data = os.path.join(str(tmp_path), "data")
    cat = os.path.join(str(tmp_path), "cat.sqlite")
    eng = create_engine(f"sqlite:///{cat}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "events", cols)
    return data, cat, eng, w


def test_inline_rows_read_timetravel_and_flush(tmp_path):
    data, cat, eng, w = _mk(tmp_path, [("id", "int64"), ("m", "varchar"),
                                       ("amt", "decimal(10,2)"), ("ts", "timestamp")])
    w.inline_rows("events", [
        {"id": 1, "m": "a", "amt": decimal.Decimal("19.95"), "ts": dt.datetime(2026, 6, 5, 12)},
        {"id": 2, "m": "b", "amt": decimal.Decimal("9.99"), "ts": dt.datetime(2026, 6, 6, 12)},
    ], snapshot_time=dt.datetime(2026, 6, 6))
    w.inline_rows("events", [
        {"id": 3, "m": "c", "amt": decimal.Decimal("5.00"), "ts": dt.datetime(2026, 6, 10, 12)},
    ], snapshot_time=dt.datetime(2026, 6, 10))
    eng.dispose()

    with dl.attach_lake(f"sqlite:{cat}", data) as c:
        assert c.execute("SELECT count(*) FROM lake.events").fetchone()[0] == 3
        assert c.execute("SELECT sum(amt) FROM lake.events").fetchone()[0] == decimal.Decimal("34.94")
        assert c.execute("SELECT ts FROM lake.events WHERE id=1").fetchone()[0] == dt.datetime(2026, 6, 5, 12)
        # time-travel: as of June 7 the June-10 batch is not yet visible
        assert c.execute("SELECT count(*) FROM lake.events "
                         "AT (TIMESTAMP => TIMESTAMP '2026-06-07')").fetchone()[0] == 2

    # native flush squishes the inlined rows into Parquet; reads stay correct
    fc = duckdb.connect()
    fc.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite")
    fc.execute(f"ATTACH 'ducklake:sqlite:{cat}' AS lake (DATA_PATH '{data}/')")
    assert fc.execute("CALL ducklake_flush_inlined_data('lake')").fetchone()[2] == 3
    assert fc.execute("SELECT count(*) FROM lake.events").fetchone()[0] == 3
    fc.execute("DETACH lake")
    assert len(glob.glob(os.path.join(data, "**", "*.parquet"), recursive=True)) == 1


def test_inline_rows_rejects_nested_columns(tmp_path):
    _, _, eng, w = _mk(tmp_path, [("id", "int64"), ("tags", "varchar[]")])
    with pytest.raises(ValueError, match="nested type"):
        w.inline_rows("events", [{"id": 1, "tags": ["x", "y"]}])
    eng.dispose()


def test_out_of_order_inline_backfill_self_canonicalizes(tmp_path):
    data, cat, eng, w = _mk(tmp_path, [("id", "int64"), ("m", "varchar")])
    # commit the later-dated batch first, the older-dated one second (a CDC backfill)
    w.inline_rows("events", [{"id": 2, "m": "june10"}], snapshot_time=dt.datetime(2026, 6, 10))
    w.inline_rows("events", [{"id": 1, "m": "june05"}], snapshot_time=dt.datetime(2026, 6, 5))
    eng.dispose()

    with dl.attach_lake(f"sqlite:{cat}", data) as c:
        # auto-canonicalize remapped the inlined begin_snapshots too, so AT() is correct
        assert c.execute("SELECT m FROM lake.events "
                         "AT (TIMESTAMP => TIMESTAMP '2026-06-07')").fetchall() == [("june05",)]
        assert c.execute("SELECT count(*) FROM lake.events "
                         "AT (TIMESTAMP => TIMESTAMP '2026-06-12')").fetchone()[0] == 2
