"""01 — Quickstart: write Parquet out-of-band, register it, read it natively.

    uv run --group dev python examples/01_quickstart.py
"""
import tempfile

from _common import Lake

lake = Lake(tempfile.mkdtemp(prefix="dl_ex01_"))
lake.create_table("readings", columns=[
    ("sensor", "varchar"), ("ts", "timestamp"), ("value", "float64"),
])

# Each append writes one Parquet file out-of-band and registers it in the catalog.
lake.append("readings", """
    SELECT * FROM (VALUES
        ('plug_a', TIMESTAMP '2026-06-20 10:00:00', 12.0),
        ('plug_a', TIMESTAMP '2026-06-20 11:00:00', 18.5)
    ) AS t(sensor, ts, value)""")

con = lake.reader()
print("rows via native ducklake reader:")
for row in con.execute("SELECT * FROM lake.readings ORDER BY ts").fetchall():
    print("  ", row)
print("\nParquet files on disk:", lake.data_files())
print("\nThe catalog (SQLite) holds only metadata; the data is the Parquet file above.")
