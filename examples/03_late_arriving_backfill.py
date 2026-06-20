"""03 — Late-arriving / federation backfill.

A node that was network-partitioned reconnects and replays OLD data after newer
data has already landed. Because we stamp each batch with its source
transaction-time, the backfill is recorded at its true point in history instead
of being smeared onto catch-up time.

    uv run --group dev python examples/03_late_arriving_backfill.py
"""
import datetime as dt
import tempfile

from _common import Lake

lake = Lake(tempfile.mkdtemp(prefix="dl_ex03_"))
lake.create_table("events", columns=[
    ("source_id", "varchar"), ("id", "int64"), ("ts", "timestamp"), ("payload", "varchar"),
])

# 1) Normal recent data from the always-on node.
lake.append("events", """SELECT * FROM (VALUES
    ('hub', 101, TIMESTAMP '2026-06-19 09:00:00', 'recent-a'),
    ('hub', 102, TIMESTAMP '2026-06-19 10:00:00', 'recent-b')) AS t(source_id,id,ts,payload)""",
    snapshot_time=dt.datetime(2026, 6, 19, 10, 0, 0))

# 2) A partitioned node reconnects TODAY and backfills data from APRIL.
#    Registered now, but stamped with the source time it actually belongs to.
lake.append("events", """SELECT * FROM (VALUES
    ('remote', 7, TIMESTAMP '2026-04-02 08:00:00', 'blast-from-the-past-1'),
    ('remote', 8, TIMESTAMP '2026-04-02 09:00:00', 'blast-from-the-past-2')) AS t(source_id,id,ts,payload)""",
    snapshot_time=dt.datetime(2026, 4, 2, 9, 0, 0))

con = lake.reader()
print("all events, ordered by their own (source) event time:")
for row in con.execute("SELECT source_id, id, ts, payload FROM lake.events ORDER BY ts").fetchall():
    print("  ", row)
print("\nThe April rows sort into April by event-time, even though they were ingested")
print("after the June rows. Snapshot timestamps preserve the source ordering:")
for sid, stime in con.execute(
        "SELECT snapshot_id, snapshot_time FROM lake.snapshots() WHERE snapshot_id>0 ORDER BY snapshot_id").fetchall():
    print(f"   snapshot {sid}: source_time={stime}")
