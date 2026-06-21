# ducklake_oob_writer

A small Python library for **out-of-band (OOB) population of [DuckLake](https://ducklake.select) catalogs**: write Parquet data files yourself, then register them directly into the DuckLake catalog tables via SQLAlchemy — never going through DuckDB's `ducklake` extension write path. The catalog can be backed by PostgreSQL, SQLite, or DuckDB. Single runtime dependency: SQLAlchemy.

> **Note:** This code is almost entirely AI-authored (Claude, Anthropic), albeit under close human supervision, and is for research and experimentation purposes. Successful experiments may be re-implemented in a more coordinated and curated manner.

## What it does

DuckLake separates a lakehouse into two parts: **immutable Parquet data files**, and a **catalog** (snapshots, schema, file lists) stored as ordinary tables in a SQL database. This library writes the *catalog* side directly, so you control the data plane independently:

- **`create_catalog(engine)`** — bootstrap the ~28 DuckLake catalog tables on any SQLAlchemy-supported database (Postgres / SQLite / DuckDB).
- **`DuckLakeWriter`** — register tables, columns, and Parquet data files into the catalog via the SQLAlchemy expression API (no raw SQL, no dialect branching).
- **`footer_and_size(path)`** — stdlib-only helper returning the `(file_size_bytes, footer_size)` a Parquet file needs to be registered.

The result is a catalog that DuckDB's native `ducklake` extension reads as if it had been written natively.

### Why out-of-band?

The headline reason is **carrying the source's transaction-time onto the DuckLake snapshot**. The native write path stamps every snapshot with the ETL process's wall-clock (`now()`); the OOB writer takes a `snapshot_time` argument so you can stamp it with the timestamp the *source system* assigned to the fact. That makes the lake **bitemporal**: `AS OF` time-travel reflects source reality, and late-arriving / backfilled data (e.g. a network-partitioned node catching up) lands at the correct point in history instead of being smeared onto catch-up time. See [`docs/DuckLake OOB Writer.md`](docs/DuckLake%20OOB%20Writer.md) for the full design and the bitemporal rationale.

## API reference

| Name | Kind | Description |
|------|------|-------------|
| `create_catalog(engine, schema=None)` | function | Create the DuckLake catalog tables on a SQLAlchemy engine |
| `_build_metadata(schema=None)` | function | Build the SQLAlchemy `MetaData` describing the catalog tables |
| `DUCKLAKE_METADATA` | constant | Default (unqualified) `MetaData` instance |
| `DUCKLAKE_VERSION` | constant | DuckLake catalog protocol version (`"1.0"`) |
| `DuckLakeWriter(engine, meta)` | class | OOB writer |
| `footer_and_size(path)` | function | `(file_size_bytes, footer_size)` for a Parquet file (stdlib-only) |
| `run_maintenance(catalog, data_path, older_than=None)` | function | Compact (attempted) + expire + cleanup; needs `[maintenance]` extra |
| `attach_lake` / `compact` / `expire_snapshots` / `cleanup_old_files` | functions | Individual maintenance ops (see [Maintenance](#maintenance)) |

### `DuckLakeWriter` methods

| Method | Description |
|--------|-------------|
| `init_catalog(data_path, version=DUCKLAKE_VERSION, author=None)` | One-time bootstrap: snapshot 0, `main` schema, metadata |
| `create_table(schema_name, table_name, columns, snapshot_time=None, ...)` | Register a new table + columns (`columns` = list of `(name, ducklake_type)`) |
| `register_data_file(table_name, path, record_count, file_size_bytes, footer_size, snapshot_time=None, column_stats=None, ...)` | Register a Parquet data file (`path` is **relative to the table directory**); pass `column_stats` for compaction-readiness |
| `register_parquet(table_name, fs_path, snapshot_time=None, ...)` | Convenience: read an on-disk Parquet file and register it **compaction-ready** (computes size/footer/record-count/column-stats via DuckDB) |
| `current_tables()` / `current_columns(table)` / `snapshots()` | Introspection |

## Installation

```bash
uv add ducklake-oob-writer
# or, for local development against a checkout:
uv add --editable ../ducklake_oob_writer
```

## Usage

```python
import duckdb
from sqlalchemy import create_engine
import ducklake_oob_writer as dl

DATA = "/lake/data"
eng = create_engine("sqlite:////lake/catalog.sqlite")
dl.create_catalog(eng)

w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
w.init_catalog(data_path=DATA)
w.create_table("main", "lts_hourly", columns=[
    ("statistic_id", "varchar"), ("ts", "timestamp"), ("mean", "float64"),
])

# write the Parquet yourself, into <DATA>/<schema>/<table>/<file>.parquet
duckdb.connect().execute(
    f"COPY (SELECT ...) TO '{DATA}/main/lts_hourly/batch-0001.parquet' (FORMAT PARQUET)")

fsize, footer = dl.footer_and_size(f"{DATA}/main/lts_hourly/batch-0001.parquet")
w.register_data_file(
    "lts_hourly", path="batch-0001.parquet",        # relative to the table dir
    record_count=n, file_size_bytes=fsize, footer_size=footer,
    snapshot_time=source_transaction_ts,            # <-- carry the SOURCE clock
)

# read it back with the native ducklake extension:
con = duckdb.connect()
con.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
con.execute(f"ATTACH 'ducklake:sqlite:/lake/catalog.sqlite' AS lake (DATA_PATH '{DATA}/')")
con.sql("SELECT * FROM lake.lts_hourly")
```

DuckLake **1.0** internal type names: `varchar`, `timestamp`, `date`, `boolean`,
`float64` (DOUBLE), `int64` (BIGINT), `int32` (INTEGER), `decimal(p,s)`.

## Maintenance

Maintenance (compaction, expiring snapshots, GC) is **delegated to DuckLake's own
engine** rather than reimplemented — it's complex and its natural timestamp is
genuinely `now()`. Needs the `duckdb` extra: `uv add "ducklake-oob-writer[maintenance]"`.

```python
import ducklake_oob_writer as dl
summary = dl.run_maintenance("sqlite:/lake/catalog.sqlite", "/lake/data",
                             older_than="2026-01-01")   # expire+cleanup; compaction attempted
```

| Function | On an OOB catalog |
|----------|-------------------|
| `expire_snapshots(catalog, data_path, older_than=...)` | ✅ works |
| `cleanup_old_files(catalog, data_path)` | ✅ works |
| `compact(catalog, data_path)` | ✅ works for **compaction-ready** files (see below) |

**Compaction readiness.** `ducklake_merge_adjacent_files` reads per-column
statistics and contiguous row-id ranges. Register files with
**`register_parquet`** (or `register_data_file(column_stats=...)`) and the writer
emits `ducklake_table_stats`, `ducklake_file_column_stats`, and the row-id ranges
that make them compactable. Files registered with the bare `register_data_file`
and no `column_stats` are still queryable but not compactable; `run_maintenance`
reports that in `summary["compact_error"]` instead of failing the pass.

## Examples

Runnable demos in [`examples/`](examples/) (use the `dev` group for `duckdb`):

```bash
uv run --group dev python examples/01_quickstart.py
```

Covers quickstart, source-time time-travel, late-arriving backfill, merge-on-read,
maintenance, and a Postgres-catalog variant. See [`examples/README.md`](examples/README.md).

## Dependencies

- **Runtime:** `sqlalchemy >= 2.0` (only).
- **Caller-provided** (not dependencies of this package): a Parquet writer such as
  `duckdb` or `pyarrow`, and the DB driver for your catalog backend
  (`psycopg2`/`psycopg` for Postgres; SQLite/DuckDB are built in).

## Constraints

- **Single writer per catalog** — catalog id counters live in the latest snapshot;
  concurrent writers collide. Funnel all writers (incl. federation feeders) through
  one ingester, or give each source its own catalog/namespace.
- **Catalog protocol coupling** — the SQLAlchemy model tracks a specific DuckLake
  catalog version (`DUCKLAKE_VERSION`). See the design doc for 1.0 interop notes.

## Consumers

- [`rule4`](https://github.com/phrrngtn/rule4) — Socrata open-data scraping into DuckLake.
- `ha_ducklake` — tailing the Home Assistant recorder into typed marts.
