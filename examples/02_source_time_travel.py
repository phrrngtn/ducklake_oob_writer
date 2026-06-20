"""02 — Source transaction-time on snapshots, and time-travel by that time.

The whole point of the OOB writer: each registration stamps the snapshot with the
SOURCE's transaction-time (not now()). Time-travel then reflects source reality.

    uv run --group dev python examples/02_source_time_travel.py
"""
import datetime as dt
import tempfile

from _common import Lake

lake = Lake(tempfile.mkdtemp(prefix="dl_ex02_"))
lake.create_table("price", columns=[("symbol", "varchar"), ("px", "float64")])

# Two batches, stamped with OLD source transaction-times (months ago), even though
# we are registering them "now".
lake.append("price", "SELECT 'ACME' AS symbol, 100.0 AS px",
            snapshot_time=dt.datetime(2026, 1, 1, 0, 0, 0))
lake.append("price", "SELECT 'ACME' AS symbol, 130.0 AS px",
            snapshot_time=dt.datetime(2026, 4, 1, 0, 0, 0))

con = lake.reader()
print("snapshots (note snapshot_time = SOURCE time, not wall-clock):")
for sid, stime in con.execute(
        "SELECT snapshot_id, snapshot_time FROM lake.snapshots() ORDER BY snapshot_id").fetchall():
    print(f"   snapshot {sid}: {stime}")

print("\nTime-travel AS OF the source timeline:")
for asof in ["2026-02-15", "2026-05-15"]:
    rows = con.execute(
        f"SELECT px FROM lake.price AT (TIMESTAMP => TIMESTAMP '{asof}')").fetchall()
    print(f"   AS OF {asof}: px = {[r[0] for r in rows]}")
print("\n=> As of mid-Feb only the Jan price exists; by mid-May the Apr price is in effect —")
print("   driven by the source's clock, regardless of when ingestion actually ran.")
