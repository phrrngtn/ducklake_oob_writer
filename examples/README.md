# Examples

Runnable demonstrations of `ducklake_oob_writer`. They use a SQLite-backed
DuckLake catalog under a temp directory (see `_common.py` for the small `Lake`
helper) and read results back with DuckDB's native `ducklake` extension.

Run with the `dev` dependency group (which provides `duckdb`):

```bash
uv run --group dev python examples/01_quickstart.py
```

| Example | Shows |
|---------|-------|
| `01_quickstart.py` | Write Parquet out-of-band → register → read it natively. The catalog holds only metadata. |
| `02_source_time_travel.py` | Snapshots carry the **source transaction-time**; `AT (TIMESTAMP => ...)` time-travels the source timeline, not wall-clock. |
| `03_late_arriving_backfill.py` | A partitioned node backfills old data; it lands at its true point in history instead of catch-up time. |
| `04_merge_on_read.py` | Append-only revisions resolved at read time with `ROW_NUMBER()` — idempotent, no in-place UPDATE. |
| `05_maintenance.py` | Compaction + cleanup via `run_maintenance` (delegated to DuckLake's engine; needs the `duckdb` extra). |
| `06_postgres_catalog.py` | Same writer code, Postgres catalog backend (for federation). Skips cleanly without a server. |

`_common.py` is a helper, not part of the public API.
