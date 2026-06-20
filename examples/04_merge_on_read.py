"""04 — Merge-on-read: revisions/duplicates resolved at query time.

The OOB writer appends immutable Parquet; it does not UPDATE in place. When a
source revises a fact (same key, newer value) you append a new batch and resolve
to the current state at read time with ROW_NUMBER() over the key, ordered by the
source time. This is idempotent: re-running a sync is harmless.

    uv run --group dev python examples/04_merge_on_read.py
"""
import datetime as dt
import tempfile

from _common import Lake

lake = Lake(tempfile.mkdtemp(prefix="dl_ex04_"))
lake.create_table("lts_hourly", columns=[
    ("statistic_id", "varchar"), ("ts", "timestamp"), ("mean", "float64"), ("src_ts", "timestamp"),
])

# Batch 1: initial hourly stats.
lake.append("lts_hourly", """SELECT * FROM (VALUES
    ('sensor.power', TIMESTAMP '2026-06-20 10:00:00', 12.0, TIMESTAMP '2026-06-20 10:05:00'),
    ('sensor.power', TIMESTAMP '2026-06-20 11:00:00', 18.0, TIMESTAMP '2026-06-20 11:05:00'))
    AS t(statistic_id, ts, mean, src_ts)""", snapshot_time=dt.datetime(2026, 6, 20, 11, 5))

# Batch 2: HA REVISES the 11:00 mean (recompute) and adds 12:00 — append, don't mutate.
lake.append("lts_hourly", """SELECT * FROM (VALUES
    ('sensor.power', TIMESTAMP '2026-06-20 11:00:00', 19.5, TIMESTAMP '2026-06-20 12:05:00'),
    ('sensor.power', TIMESTAMP '2026-06-20 12:00:00', 22.0, TIMESTAMP '2026-06-20 12:05:00'))
    AS t(statistic_id, ts, mean, src_ts)""", snapshot_time=dt.datetime(2026, 6, 20, 12, 5))

con = lake.reader()
print("raw rows (both versions of 11:00 present — append-only):")
for r in con.execute("SELECT ts, mean, src_ts FROM lake.lts_hourly ORDER BY ts, src_ts").fetchall():
    print("  ", r)

print("\ncurrent state via merge-on-read (latest src_ts wins per (statistic_id, ts)):")
rows = con.execute("""
    SELECT statistic_id, ts, mean FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY statistic_id, ts ORDER BY src_ts DESC) AS rn
        FROM lake.lts_hourly)
    WHERE rn = 1 ORDER BY ts""").fetchall()
for r in rows:
    print("  ", r)
print("\n=> 11:00 resolves to the revised 19.5; nothing was updated in place.")
