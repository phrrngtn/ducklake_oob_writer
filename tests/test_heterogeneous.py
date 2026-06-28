"""One DuckLake catalog can reference data files scattered across backends.

DuckLake's `path_is_relative` flag lets a data file's path be a full URI that escapes
DATA_PATH. The OOB writer detects an absolute path/URI and records it verbatim
(path_is_relative=False), so a single catalog can union files from different
locations — proven by a native-reader round-trip over a relative file under DATA_PATH
plus an absolute file on a different root (a stand-in for another S3 endpoint).

Run: uv run --group dev pytest tests/test_heterogeneous.py -v
"""
import datetime as dt
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine, text

import ducklake_oob_writer as dl


def test_one_catalog_unions_files_from_different_backends(tmp_path):
    data = os.path.join(str(tmp_path), "data")             # DATA_PATH (one "backend")
    other = os.path.join(str(tmp_path), "other_backend")   # a different root (another)
    cat = os.path.join(str(tmp_path), "cat.sqlite")
    os.makedirs(os.path.join(data, "main", "facts"))
    os.makedirs(other)

    dw = duckdb.connect()
    rel_f = os.path.join(data, "main", "facts", "local.parquet")
    dw.execute(f"COPY (SELECT 'local' AS m, range AS id FROM range(0,4)) TO '{rel_f}' (FORMAT PARQUET)")
    abs_f = os.path.join(other, "remote.parquet")
    dw.execute(f"COPY (SELECT 'remote' AS m, range AS id FROM range(0,6)) TO '{abs_f}' (FORMAT PARQUET)")
    dw.close()

    eng = create_engine(f"sqlite:///{cat}")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "facts", [("m", "varchar"), ("id", "int64")],
                   snapshot_time=dt.datetime(2026, 6, 1))
    # relative (stored under the table dir) and absolute (stored verbatim, off-DATA_PATH)
    w.register_parquet("facts", rel_f, rel_path="local.parquet", snapshot_time=dt.datetime(2026, 6, 5))
    w.register_parquet("facts", abs_f, snapshot_time=dt.datetime(2026, 6, 6))

    with eng.connect() as c:
        flags = dict(c.execute(text(
            "SELECT path, path_is_relative FROM ducklake_data_file ORDER BY path")).fetchall())
    eng.dispose()
    assert flags == {"local.parquet": 1, abs_f: 0}     # relative kept relative; URL kept absolute

    with dl.attach_lake(f"sqlite:{cat}", data) as rc:
        assert dict(rc.execute("SELECT m, count(*) FROM lake.facts GROUP BY 1 ORDER BY 1"
                               ).fetchall()) == {"local": 4, "remote": 6}
