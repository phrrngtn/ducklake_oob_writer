"""Regression tests for native DuckDB compaction of OOB-written files.

Two guarantees are enforced here:

1. **Correctness** — DuckDB's *native* compaction (`ducklake_merge_adjacent_files`)
   reads files registered by the OOB writer and produces a correct, consistent,
   still-queryable result (exact data incl. NULLs/multiple types, real file
   reduction, valid merged Parquet, contiguous row-ids, working time-travel,
   re-append + re-compact).

2. **Boundary** — the package never reads/merges/rewrites data files itself; the
   maintenance functions are thin `CALL`s into DuckDB's native `ducklake` engine.

Run: uv run --group dev pytest tests/test_native_compaction.py -v
"""
import datetime as dt
import glob
import hashlib
import inspect
import os

import pytest

pytest.importorskip("duckdb")
import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl

N_FILES = 12
ROWS_PER_FILE = 5
TOTAL = N_FILES * ROWS_PER_FILE


def _digest(rows):
    return hashlib.sha256(repr(rows).encode()).hexdigest()


def _read_current(catalog, data):
    with dl.attach_lake(catalog, data) as c:
        return c.execute("SELECT id,name,ts,amount,flag FROM lake.m ORDER BY id").fetchall()


def _build_lake(workdir):
    """Build an OOB-written DuckLake with multi-type, partly-NULL data across
    N_FILES tiny Parquet files. Returns (catalog, data_path)."""
    data = os.path.join(workdir, "data")
    os.makedirs(os.path.join(data, "main", "m"), exist_ok=True)
    catalog = f"sqlite:{workdir}/cat.sqlite"
    eng = create_engine(f"sqlite:///{workdir}/cat.sqlite")
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "m", columns=[
        ("id", "int64"), ("name", "varchar"), ("ts", "timestamp"),
        ("amount", "float64"), ("flag", "boolean"),
    ])
    dw = duckdb.connect()
    for f in range(N_FILES):
        vals = []
        for r in range(ROWS_PER_FILE):
            i = f * ROWS_PER_FILE + r
            name = "NULL" if i % 7 == 0 else f"'item_{i}'"          # exercise NULL varchar
            amt = "NULL" if i % 11 == 0 else f"{i * 1.25}::double"  # exercise NULL double
            flag = "true" if i % 2 == 0 else "false"
            vals.append(f"({i}::bigint, {name}, "
                        f"TIMESTAMP '2026-06-20 00:00:00' + INTERVAL ({i}) MINUTE, {amt}, {flag})")
        fp = os.path.join(data, "main", "m", f"batch-{f:04d}.parquet")
        dw.execute(f"COPY (SELECT * FROM (VALUES {','.join(vals)}) "
                   f"AS t(id,name,ts,amount,flag)) TO '{fp}' (FORMAT PARQUET)")
        w.register_parquet("m", fp, snapshot_time=dt.datetime(2026, 6, 20) + dt.timedelta(minutes=f))
    dw.close()
    eng.dispose()
    return catalog, data


@pytest.fixture(scope="module")
def before(tmp_path_factory):
    workdir = str(tmp_path_factory.mktemp("oob_compact"))
    catalog, data = _build_lake(workdir)
    rows = _read_current(catalog, data)
    with dl.attach_lake(catalog, data) as c:
        max_ver = c.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    return {"catalog": catalog, "data": data, "rows": rows, "digest": _digest(rows),
            "max_ver": max_ver, "files": len(glob.glob(f"{data}/main/m/*.parquet"))}


@pytest.fixture(scope="module")
def compacted(before):
    dl.compact(before["catalog"], before["data"])              # <-- NATIVE compaction
    cleaned = dl.cleanup_old_files(before["catalog"], before["data"])
    rows = _read_current(before["catalog"], before["data"])
    with dl.attach_lake(before["catalog"], before["data"]) as c:
        live = c.execute(
            "SELECT path, row_id_start, record_count "
            "FROM __ducklake_metadata_lake.ducklake_data_file "
            "WHERE end_snapshot IS NULL ORDER BY row_id_start").fetchall()
    return {"rows": rows, "digest": _digest(rows), "cleaned": len(cleaned), "live": live,
            "files": len(glob.glob(f"{before['data']}/main/m/*.parquet"))}


def test_exact_data_preserved(before, compacted):
    assert len(compacted["rows"]) == TOTAL
    assert compacted["digest"] == before["digest"]


def test_file_count_dropped(before, compacted):
    assert compacted["files"] < before["files"]
    assert compacted["files"] == 1
    assert compacted["cleaned"] == before["files"]


def test_merged_file_is_valid_parquet_read_directly(before, compacted):
    total = 0
    for path, _start, _cnt in compacted["live"]:
        fp = os.path.join(before["data"], "main", "m", path)
        total += duckdb.connect().execute("SELECT count(*) FROM read_parquet(?)", [fp]).fetchone()[0]
    assert total == TOTAL


def test_row_ids_contiguous_and_non_overlapping(compacted):
    covered = []
    for _path, start, cnt in compacted["live"]:
        covered.extend(range(start, start + cnt))
    assert sorted(covered) == list(range(TOTAL))


def test_time_travel_to_pre_compaction_snapshot(before, compacted):
    with dl.attach_lake(before["catalog"], before["data"]) as c:
        n = c.execute(f"SELECT count(*) FROM lake.m AT (VERSION => {before['max_ver']})").fetchone()[0]
    assert n == TOTAL


def test_reappend_after_native_compaction(tmp_path):
    """A fresh writer can append more OOB files after a native compaction, and a
    second native compaction stays correct."""
    catalog, data = _build_lake(str(tmp_path))
    dl.compact(catalog, data)
    dl.cleanup_old_files(catalog, data)
    assert len(_read_current(catalog, data)) == TOTAL

    eng = create_engine(f"sqlite:///{tmp_path}/cat.sqlite")  # fresh writer reloads counters
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    dw = duckdb.connect()
    for f in range(3):
        i = 1000 + f
        fp = os.path.join(data, "main", "m", f"late-{f:04d}.parquet")
        dw.execute(f"COPY (SELECT {i}::bigint AS id, 'late_{i}' AS name, "
                   f"TIMESTAMP '2026-07-01' AS ts, {i * 1.0}::double AS amount, true AS flag) "
                   f"TO '{fp}' (FORMAT PARQUET)")
        w.register_parquet("m", fp, snapshot_time=dt.datetime(2026, 7, 1) + dt.timedelta(minutes=f))
    dw.close()
    eng.dispose()
    assert len(_read_current(catalog, data)) == TOTAL + 3

    dl.compact(catalog, data)
    dl.cleanup_old_files(catalog, data)
    assert len(_read_current(catalog, data)) == TOTAL + 3


def test_maintenance_is_delegated_to_native_not_reimplemented():
    """Boundary guard: maintenance is performed by DuckDB's native `ducklake`
    engine via CALLs. The package must not implement compaction (merging/rewriting
    data files) in Python — if someone does, this fails.

    Note: reading a single Parquet file to compute *statistics* (in `register_parquet`)
    is allowed — that's metadata, not compaction. The boundary is about the
    maintenance module never touching data files, and the writer never compacting.
    """
    import ducklake_oob_writer.maintenance as _m
    import ducklake_oob_writer.writer as _w

    # The maintenance ops are thin CALLs into the native ducklake engine ...
    assert "ducklake_merge_adjacent_files" in inspect.getsource(dl.compact)
    assert "ducklake_expire_snapshots" in inspect.getsource(dl.expire_snapshots)
    assert "ducklake_cleanup_old_files" in inspect.getsource(dl.cleanup_old_files)
    # ... and the maintenance module itself never reads or rewrites data files.
    msrc = inspect.getsource(_m)
    assert "read_parquet" not in msrc
    assert "COPY " not in msrc
    # The writer emits catalog metadata/stats only; it never compacts.
    assert "ducklake_merge_adjacent_files" not in inspect.getsource(_w)
