"""Additive schema evolution — DuckLake-native ALTER ADD COLUMN, out-of-band.

add_column appends a column (new schema_version); pre-existing data files read NULL for
it. reconcile_columns is the diff->evolve step a schema-as-data driver (column_role) feeds.

Run: uv run --group dev pytest tests/test_schema_evolution.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl


def test_add_column_and_reconcile(tmp_path):
    data = os.path.join(str(tmp_path), "data")
    cat = os.path.join(str(tmp_path), "cat.sqlite")
    os.makedirs(os.path.join(data, "main", "t"))
    f1 = os.path.join(data, "main", "t", "f1.parquet")
    f2 = os.path.join(data, "main", "t", "f2.parquet")
    duckdb.connect().execute(f"COPY (SELECT 1 AS id, 'a' AS name) TO '{f1}' (FORMAT PARQUET)")

    eng = create_engine(f"sqlite:///{cat}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "t", [("id", "int64"), ("name", "varchar")])
    w.register_parquet("t", f1, rel_path="f1.parquet", snapshot_time=dt.datetime(2026, 6, 29, 10))

    # evolve: add a column mid-stream
    res = w.add_column("t", "region", "varchar", snapshot_time=dt.datetime(2026, 6, 29, 11))
    assert res["schema_version"] == 2   # created_schema(0) -> created_table(1) -> altered(2)

    # a new file carrying the wider schema
    duckdb.connect().execute(
        f"COPY (SELECT 2 AS id, 'b' AS name, 'X' AS region) TO '{f2}' (FORMAT PARQUET)")
    w.register_parquet("t", f2, rel_path="f2.parquet", snapshot_time=dt.datetime(2026, 6, 29, 12))

    # reconcile against a desired column set (what column_role would supply): region is
    # already present, so only the genuinely new column is added
    added = w.reconcile_columns("t", [("id", "int64"), ("name", "varchar"),
                                      ("region", "varchar"), ("city", "varchar")])
    assert added == ["city"]
    eng.dispose()

    with dl.attach_lake(f"sqlite:{cat}", data) as c:
        rows = c.execute("SELECT id, name, region, city FROM lake.t ORDER BY id").fetchall()
        # pre-ALTER row reads NULL for region; both rows read NULL for the just-added city
        assert rows == [(1, "a", None, None), (2, "b", "X", None)]
        cols = [r[0] for r in c.execute("DESCRIBE lake.t").fetchall()]
        assert cols == ["id", "name", "region", "city"]
