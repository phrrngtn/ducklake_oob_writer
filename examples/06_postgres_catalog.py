"""06 — Same OOB pattern, but with a PostgreSQL catalog backend.

Only the catalog *engine* changes; the writer code is identical. This is what you
use for multi-source federation (Postgres arbitrates concurrent commits) — while
the data still lives as plain Parquet files.

Requires a reachable Postgres and `psycopg2`/`psycopg`. Set DDL_PG_DSN, e.g.:
    DDL_PG_DSN='postgresql+psycopg://user:pw@localhost:5432/lake' \
    DUCKLAKE_PG='postgres:dbname=lake host=localhost user=user password=pw' \
    DATA_DIR=/tmp/dl_pg_data \
    uv run --group dev python examples/06_postgres_catalog.py

Skips cleanly if DDL_PG_DSN is not set.
"""
import datetime as dt
import os
import sys

sa_dsn = os.environ.get("DDL_PG_DSN")
if not sa_dsn:
    print("DDL_PG_DSN not set — skipping the Postgres example (no server to talk to).")
    sys.exit(0)

import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl

ducklake_pg = os.environ["DUCKLAKE_PG"]          # e.g. 'postgres:dbname=lake host=localhost ...'
data_dir = os.environ.get("DATA_DIR", "/tmp/dl_pg_data")
os.makedirs(f"{data_dir}/main/price", exist_ok=True)

engine = create_engine(sa_dsn)
dl.create_catalog(engine)
w = dl.DuckLakeWriter(engine, dl.DUCKLAKE_METADATA)
w.init_catalog(data_path=data_dir)
w.create_table("main", "price", columns=[("symbol", "varchar"), ("px", "float64")])

phys = f"{data_dir}/main/price/batch-0001.parquet"
duckdb.connect().execute(f"COPY (SELECT 'ACME' AS symbol, 100.0 AS px) TO '{phys}' (FORMAT PARQUET)")
fsize, footer = dl.footer_and_size(phys)
w.register_data_file("price", path="batch-0001.parquet", record_count=1,
                     file_size_bytes=fsize, footer_size=footer,
                     snapshot_time=dt.datetime(2026, 1, 1))
engine.dispose()

con = duckdb.connect()
con.execute("INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;")
con.execute(f"ATTACH 'ducklake:{ducklake_pg}' AS lake (DATA_PATH '{data_dir}/')")
print("rows from Postgres-backed DuckLake:", con.execute("SELECT * FROM lake.price").fetchall())
