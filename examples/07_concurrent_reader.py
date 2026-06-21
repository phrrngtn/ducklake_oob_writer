"""07 — Concurrent reader: a DuckDB connection reads the lake while the OOB writer
is actively writing. Concurrent readers are safe — a live reader sees the writer's
committed snapshots appear, one at a time, with no blocking and no errors.

    uv run --group dev python examples/07_concurrent_reader.py
"""
import datetime as dt
import os
import sqlite3
import tempfile
import threading
import time

import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl

W = tempfile.mkdtemp(prefix="dl_ex07_")
DATA = f"{W}/data"; os.makedirs(f"{DATA}/main/t", exist_ok=True)
CF = f"{W}/cat.sqlite"; CAT = f"sqlite:{CF}"
# WAL so the reader never blocks behind the writer's commits. (Safe here: only the
# OOB writer writes the catalog; DuckDB only reads. See example 08 for why you must
# NOT use WAL when DuckDB also *writes* the catalog.)
sqlite3.connect(CF).execute("PRAGMA journal_mode=WAL").close()

eng = create_engine(f"sqlite:///{CF}")
dl.create_catalog(eng)
w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
w.init_catalog(data_path=DATA)
w.create_table("main", "t", columns=[("i", "int64")])
dw = duckdb.connect()

# Reader thread: a separate DuckDB connection, ATTACHed READ_ONLY, polling row count.
observed = []
def reader():
    c = duckdb.connect()
    c.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
    c.execute(f"ATTACH 'ducklake:{CAT}' AS lake (DATA_PATH '{DATA}/', READ_ONLY)")
    for _ in range(16):
        observed.append(c.execute("SELECT count(*) FROM lake.t").fetchone()[0])
        time.sleep(0.05)
    c.close()

t = threading.Thread(target=reader)
t.start()

print("OOB writer committing 10 files while a separate DuckDB reader polls...")
for i in range(10):
    fp = f"{DATA}/main/t/b{i:03d}.parquet"
    dw.execute(f"COPY (SELECT {i}::bigint AS i) TO '{fp}' (FORMAT PARQUET)")
    w.register_parquet("t", fp, snapshot_time=dt.datetime(2026, 6, 20) + dt.timedelta(minutes=i))
    time.sleep(0.06)
t.join()
dw.close()

print("row counts the live reader observed over time:", observed)
print("\n=> the reader (a separate DuckDB attach) sees the writer's commits appear")
print("   incrementally, with no blocking and no errors. Concurrent readers are safe.")
