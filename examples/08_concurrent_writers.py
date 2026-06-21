"""08 — Concurrent writers: the OOB writer assumes it is the SOLE writer of the
catalog (it caches catalog id counters). This shows:

  (1) native DuckDB and the OOB writer can take TURNS safely (serialize + use a
      fresh writer that re-reads the counters), and
  (2) if another writer commits BETWEEN OOB operations, the OOB writer's cached
      counters go stale — but it FAILS LOUD (snapshot_id PK -> rollback), it does
      NOT silently corrupt the catalog.

Uses a SQLite catalog in the DEFAULT journal mode. (Do NOT use WAL when DuckDB
also *writes* the catalog: two different SQLite implementations on one WAL file
can corrupt it. A Postgres catalog avoids every file-level hazard — see the
"Concurrency" section of the README.)

    uv run --group dev python examples/08_concurrent_writers.py
"""
import datetime as dt
import os
import tempfile

import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl

W = tempfile.mkdtemp(prefix="dl_ex08_")
DATA = f"{W}/data"; os.makedirs(f"{DATA}/main/t", exist_ok=True)
CF = f"{W}/cat.sqlite"; CAT = f"sqlite:{CF}"   # default journal (NOT WAL)
eng = create_engine(f"sqlite:///{CF}")
dl.create_catalog(eng)
w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
w.init_catalog(data_path=DATA)
w.create_table("main", "t", columns=[("i", "int64")])
dw = duckdb.connect()


def oob(writer, i):
    fp = f"{DATA}/main/t/b{i}.parquet"
    dw.execute(f"COPY (SELECT {i}::bigint AS i) TO '{fp}' (FORMAT PARQUET)")
    return writer.register_parquet("t", fp, snapshot_time=dt.datetime(2026, 6, 20) + dt.timedelta(minutes=i))


def native_insert(val):
    c = duckdb.connect()
    c.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
    c.execute(f"ATTACH 'ducklake:{CAT}' AS lake (DATA_PATH '{DATA}/')")
    c.execute(f"INSERT INTO lake.t VALUES ({val})")
    c.close()


def count():
    with dl.attach_lake(CAT, DATA) as c:
        return c.execute("SELECT count(*) FROM lake.t").fetchone()[0]


print("PART 1 — taking TURNS (safe):")
oob(w, 1);            print("  OOB write        -> rows", count())
native_insert(1000); print("  native DuckDB    -> rows", count())
# a FRESH writer re-reads the id counters from the catalog, so it continues cleanly
w_fresh = dl.DuckLakeWriter(create_engine(f"sqlite:///{CF}"), dl.DUCKLAKE_METADATA)
oob(w_fresh, 2);     print("  fresh OOB write  -> rows", count())
print("  => native and OOB interleave fine when SERIALIZED + the writer is re-created.")

print("\nPART 2 — STALE writer after an external write (unsafe, but fails loud):")
native_insert(2000); print("  native DuckDB writes again -> rows", count())
print("  reuse the OLD 'w' instance (its cached counters are now stale)...")
try:
    oob(w, 3)
    print("  stale OOB write SUCCEEDED (unexpected!)")
except Exception as e:
    print("  stale OOB write FAILED:", str(e).splitlines()[0])
print("  rows still readable:", count(), "(failed txn rolled back; catalog intact)")

dw.close()
print("""
Takeaways
  - The OOB writer OWNS the catalog (single-writer): it caches id counters.
  - An external write between its operations makes it stale -- but the
    snapshot_id PRIMARY KEY turns that into a LOUD failure (rollback), never
    silent corruption.
  - Safe patterns: one writer; or serialize and re-create the writer between
    external writes; or use a Postgres catalog (server-arbitrated, no file
    corruption possible).
""")
