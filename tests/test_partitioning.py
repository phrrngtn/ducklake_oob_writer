"""Tests for OOB-written DuckLake partitioning.

Two guarantees:

1. **OOB catalog** — ``set_partitioning`` records a native partition spec
   (``ducklake_partition_info`` / ``ducklake_partition_column``), and
   ``register_data_file`` stamps each file's ``partition_id`` and derives its
   ``ducklake_file_partition_value`` from the file's *own* constant column
   (``min == max`` in the stats) — no Parquet is rewritten.

2. **Native reader** — DuckDB's native ``ducklake`` engine reads the result: it
   materializes the partition column and **prunes** files on a partition
   predicate (``Total Files Read: 1`` of 2).

Run: uv run --group dev pytest tests/test_partitioning.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine, text

import ducklake_oob_writer as dl


def _build_partitioned_lake(workdir):
    """Two pre-existing Parquet files (one event_date each), partitioned by
    event_date and registered OOB. Returns (catalog, data_path, spec)."""
    data = os.path.join(workdir, "data")
    os.makedirs(os.path.join(data, "main", "events"), exist_ok=True)
    catalog = f"sqlite:{workdir}/cat.sqlite"
    eng = create_engine(f"sqlite:///{workdir}/cat.sqlite")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "events", columns=[
        ("event_date", "date"), ("sensor", "varchar"), ("value", "float64")])
    spec = w.set_partitioning("events", ["event_date"])
    dw = duckdb.connect()
    for d, lo, hi in [("2026-06-26", 0, 5000), ("2026-06-27", 5000, 10000)]:
        f = os.path.join(data, "main", "events", f"events-{d}.parquet")
        dw.execute(
            f"COPY (SELECT DATE '{d}' AS event_date, 'sensor_'||(range%4) AS sensor, "
            f"(range*1.5)::DOUBLE AS value FROM range({lo},{hi})) TO '{f}' (FORMAT PARQUET)")
        w.register_parquet("events", f)
    dw.close()
    eng.dispose()
    return catalog, data, spec


def test_oob_writer_records_partition_spec_and_values(tmp_path):
    _, _, spec = _build_partitioned_lake(str(tmp_path))
    pid = spec["partition_id"]
    eng = create_engine(f"sqlite:///{tmp_path}/cat.sqlite")
    with eng.connect() as con:
        files = con.execute(text(
            "SELECT data_file_id, partition_id FROM ducklake_data_file ORDER BY 1")).fetchall()
        pvals = con.execute(text(
            "SELECT data_file_id, partition_key_index, partition_value "
            "FROM ducklake_file_partition_value ORDER BY 1")).fetchall()
        pcols = con.execute(text(
            "SELECT partition_id, partition_key_index, transform "
            "FROM ducklake_partition_column")).fetchall()
    eng.dispose()
    assert files == [(0, pid), (1, pid)]                            # spec id stamped on each file
    assert pvals == [(0, 0, "2026-06-26"), (1, 0, "2026-06-27")]    # value per file, from min==max
    assert pcols == [(pid, 0, "identity")]


def test_native_reader_materializes_and_prunes(tmp_path):
    catalog, data, _ = _build_partitioned_lake(str(tmp_path))
    with dl.attach_lake(catalog, data) as c:
        assert c.execute(
            "SELECT event_date, count(*) FROM lake.events GROUP BY event_date ORDER BY 1"
        ).fetchall() == [(dt.date(2026, 6, 26), 5000), (dt.date(2026, 6, 27), 5000)]
        assert c.execute(
            "SELECT count(*) FROM lake.events WHERE event_date = DATE '2026-06-27'"
        ).fetchone()[0] == 5000
        plan = c.execute(
            "EXPLAIN ANALYZE SELECT count(*) FROM lake.events WHERE event_date = DATE '2026-06-27'"
        ).fetchall()[-1][-1]
        assert "Total Files Read: 1" in plan
