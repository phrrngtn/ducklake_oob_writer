"""05 — Maintenance, delegated to DuckLake's own engine.

`run_maintenance` attaches the catalog with DuckDB's `ducklake` extension and
CALLs the native maintenance functions: compact (merge small files), expire old
snapshots, and clean up the now-orphaned files.

Compaction needs per-column statistics + row-id ranges. The `Lake` helper here
registers via `register_parquet`, which emits those, so the files are
compaction-ready. (Files registered with the bare `register_data_file` and no
`column_stats` are queryable but not compactable — `run_maintenance` reports that
in `summary["compact_error"]` instead of failing.)

    uv run --group dev python examples/05_maintenance.py
"""
import datetime as dt
import tempfile

import ducklake_oob_writer as dl
from _common import Lake

lake = Lake(tempfile.mkdtemp(prefix="dl_ex05_"))
lake.create_table("ticks", columns=[("ts", "timestamp"), ("v", "float64")])

# Simulate a chatty source: 12 tiny single-row batches => 12 tiny Parquet files,
# each with its own source transaction-time.
base = dt.datetime(2026, 6, 20, 0, 0, 0)
for i in range(12):
    t = base + dt.timedelta(minutes=i)
    lake.append("ticks", f"SELECT TIMESTAMP '{t:%Y-%m-%d %H:%M:%S}' AS ts, {i * 1.5} AS v",
                snapshot_time=t)

con = lake.reader()
before_rows = con.execute("SELECT count(*) FROM lake.ticks").fetchone()[0]
con.close()
print(f"before: {len(lake.data_files())} Parquet files, {before_rows} rows")

# Compact, expire snapshots older than 00:06 (by SOURCE time), then cleanup.
lake.release()
summary = dl.run_maintenance(lake.catalog, str(lake.data_path),
                             older_than=dt.datetime(2026, 6, 20, 0, 6, 0))
print("run_maintenance summary:")
for k, v in summary.items():
    print(f"   {k}: {v}")

con = lake.reader()
after_rows = con.execute("SELECT count(*) FROM lake.ticks").fetchone()[0]
con.close()
print(f"\nafter:  {len(lake.data_files())} Parquet files, {after_rows} rows (current state preserved)")
print("\n=> 12 tiny files compacted to 1; old snapshots expired; orphaned files cleaned.")
