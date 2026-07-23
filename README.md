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

The headline reason is **carrying the source's transaction-time onto the DuckLake snapshot**. The native write path stamps every snapshot with the ETL process's wall-clock (`now()`); the OOB writer takes a `snapshot_time` argument so you can stamp it with the timestamp the *source system* assigned to the fact. That makes the lake **bitemporal**: `AS OF` time-travel can reflect source reality, and late-arriving / backfilled data (e.g. a network-partitioned node catching up) can land at the correct point in history rather than being smeared onto catch-up time — *provided the snapshots are kept in transaction-time order* (see [Out-of-order arrivals](#out-of-order-arrivals--time-travel) below). See [`docs/DuckLake OOB Writer.md`](docs/DuckLake%20OOB%20Writer.md) for the original design and the bitemporal rationale.

## Out-of-order arrivals & time-travel

Stamping `snapshot_time` with the source's transaction-time is necessary but **not sufficient** for correct `AS OF` time-travel. DuckLake orders a table's state by the surrogate `snapshot_id` (insertion order), *not* by `snapshot_time` — so a **late-arriving / backfilled** fact, written *after* later-dated data, lands at a higher `snapshot_id` and makes `AT (TIMESTAMP)` wrong in *both* directions: it leaks future-dated rows in, and drops the backfill out.

The fix is **automatic**: `register_data_file` keeps `snapshot_id` order aligned with `snapshot_time` order **in the same transaction**, so adding files just stays safe — callers never invoke anything. The mechanism is *generate the out-of-order snapshots, then renumber them*: an idempotent, set-based DML batch — `ROW_NUMBER()` to rank (schema snapshots first, then data by `snapshot_time`), a transient `violators` table holding only the snapshots whose id changes, plain `UPDATE`s to remap the references, and an offset move for the two `snapshot_id`-keyed tables (DuckLake declares no foreign keys, so there's no ordering to respect). When nothing arrived out of order the `violators` set is empty and every statement is a no-op. It's written with SQLAlchemy Core so one implementation is dialect-portable (SQLite / PostgreSQL / DuckDB), touches only the moved snapshots and their references, leaves the dimension columns untouched (no snapshot id), and the native reader ignores `row_id_start`/the allocation counters — so it's the snapshot-id remap alone. The standalone `recanonicalize(engine)` is still exported if you ever want to run it by hand.

This — together with the **path-vs-rewrite dimension encoding** for partition/pruning columns, and the surrogate-vs-natural-key reasoning behind the whole approach — is written up in full in **[`docs/Temporal and Partitioning Design.md`](docs/Temporal%20and%20Partitioning%20Design.md)**.

### Opt-out: `reject_out_of_order` (monotonic mode)

`DuckLakeWriter(engine, meta, reject_out_of_order=True)` flips the policy: instead of *accepting* an out-of-order arrival and renumbering, it **refuses** any data/inline write whose `snapshot_time` precedes an existing snapshot **for the same table** (`ValueError`). The guard is **per-table**, not global — independently-tailed tables advance on their own source clocks, so table B's legitimately-earlier arrival is never blocked by table A. With no OOO possible, `snapshot_id` order can never diverge from `snapshot_time` order, the automatic renumber becomes a guaranteed no-op (and is skipped), and **`snapshot_id` is a stable cursor** — a downstream client (e.g. a per-table SQLite TTST subscriber cursoring on `ducklake_table_changes` by a stored HWM) can rely on it without a redundant backlog. The trade: you forgo honest late-arriving backfill, so it's a per-lake choice — a single-source-per-table tail-replica lake runs monotonic; a multi-clock-domain federation lake keeps the default accept-OOO + canonicalize. Default is `False` (backward-compatible).

## Inlined data — small writes with no files

For small, frequent arrivals (e.g. **CDC/CT polling since a high-water mark**) a tiny Parquet file per batch is the wrong move — it creates the small-files problem. DuckLake's answer is **data inlining**: the rows live *directly in the catalog database* (in `ducklake_inlined_data_<table_id>_<schema_version>`, with MVCC bookkeeping `row_id` / `begin_snapshot` / `end_snapshot`), and reads union them with the table's Parquet files.

`inline_rows(table, rows)` writes them **out-of-band with nothing but SQLAlchemy** — no Parquet, no DuckDB, no object store. A process with only a database connection can populate a queryable lake. It auto-canonicalizes (so an **out-of-order inlined backfill** — the CDC late-arrival — time-travels correctly), and you later **squish the inlined rows down to Parquet** with the native `CALL ducklake_flush_inlined_data('lake')`, then compact with `ducklake_merge_adjacent_files`. The whole loop: *poll-since-HWM → `inline_rows` → flush*.

**Scalar / simple-typed columns only** — int, double, varchar, boolean, decimal, date/time/timestamp. A nested column (`LIST`/`STRUCT`/`MAP`) raises: route those through Parquet, where types are exact and portable.

Why the restriction: inlined data is rows in the catalog DB, so values must fit its columns. In the normal path **DuckDB is both the writer and the reader** of inlined data — it stores non-native types as *its own* `CAST(… AS VARCHAR)` text and parses that back, self-consistent. The OOB writer is the unusual writer, so it must **reproduce DuckDB's representation, not invent one**. For the simple types Python's `str()` reproduces it *byte-for-byte* — a differential test writes the same rows via native DuckDB and via `inline_rows` and asserts the catalog is identical. Nested types serialize to DuckDB's bespoke literal form (`[x, y]`, `{'a': 1}`), not safe to reproduce from outside the engine — hence refused.

## API reference

| Name | Kind | Description |
|------|------|-------------|
| `create_catalog(engine, schema=None)` | function | Create the DuckLake catalog tables on a SQLAlchemy engine |
| `_build_metadata(schema=None)` | function | Build the SQLAlchemy `MetaData` describing the catalog tables |
| `DUCKLAKE_METADATA` | constant | Default (unqualified) `MetaData` instance |
| `DUCKLAKE_VERSION` | constant | DuckLake catalog protocol version (`"1.0"`) |
| `DuckLakeWriter(engine, meta, *, reject_out_of_order=False)` | class | OOB writer; `reject_out_of_order=True` enforces per-table transaction-time monotonicity (refuse OOO instead of canonicalizing) so `snapshot_id` stays a stable downstream cursor |
| `footer_and_size(path)` | function | `(file_size_bytes, footer_size)` for a Parquet file (stdlib-only) |
| `run_maintenance(catalog, data_path, older_than=None)` | function | Compact (attempted) + expire + cleanup; needs `[maintenance]` extra |
| `attach_lake` / `compact` / `expire_snapshots` / `cleanup_old_files` | functions | Individual maintenance ops (see [Maintenance](#maintenance)) |

### `DuckLakeWriter` methods

| Method | Description |
|--------|-------------|
| `init_catalog(data_path, version=DUCKLAKE_VERSION, author=None)` | One-time bootstrap: snapshot 0, `main` schema, metadata |
| `create_table(schema_name, table_name, columns, snapshot_time=None, ...)` | Register a new table + columns (`columns` = list of `(name, ducklake_type)`) |
| `register_data_file(table_name, path, record_count, file_size_bytes, footer_size, snapshot_time=None, column_stats=None, ...)` | Register a Parquet data file. A bare `path` is **relative to the table directory** under `DATA_PATH`; an **absolute path or URI** (`s3://…`, `gs://…`, `/abs/…`) is stored verbatim (`path_is_relative=False`) so one catalog can union files **scattered across backends** (the reader just needs a secret per endpoint). Pass `column_stats` to add query-pruning stats |
| `register_parquet(table_name, fs_path, snapshot_time=None, ...)` | Convenience: read an on-disk Parquet file and register it, computing size/footer/record-count + per-column *pruning* stats via DuckDB |
| `inline_rows(table_name, rows, snapshot_time=None, ...)` | Write rows **straight into the catalog** as DuckLake inlined data — **no Parquet, no DuckDB** — for small/frequent arrivals (CDC/CT). Scalar/simple-typed columns only; flush to Parquet later with the native `ducklake_flush_inlined_data` |
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
| `compact(catalog, data_path)` | ✅ works for any registered file (see below) |

**What compaction actually needs.** `ducklake_merge_adjacent_files`
(`GetFilesForCompaction`) reads each table's per-table `ducklake_schema_versions`
row by `table_id`, and `create_table` always emits it — so **any file registered
through `create_table` + `register_data_file` is compactable**. `column_stats` /
`register_parquet` are **not** required for compaction; they add per-column
statistics for query *pruning*. The only catalogs that can't compact are ones
missing the per-table `schema_versions` row (e.g. written by a pre-fix version of
this package, or another tool); `run_maintenance` reports that in
`summary["compact_error"]` instead of failing the pass. See the design doc for the
DuckLake-source citation.

### Boundary

**This package never reads, merges, or rewrites Parquet data files for
compaction.** The actual data-file work is performed entirely by DuckDB's native
`ducklake` engine — `compact`, `expire_snapshots`, and `cleanup_old_files` each
just `ATTACH` the catalog and issue a single `CALL ducklake_*(...)`. The package's
*only* contribution to compaction is to **enable** it: the writer emits, at
registration time, the per-table `ducklake_schema_versions` row the native
compaction planner reads (plus row-id bookkeeping). (Reading a Parquet file to
*compute* pruning stats in `register_parquet` is metadata work, not compaction.)
The boundary is enforced by `tests/test_native_compaction.py`, which also verifies
native compaction of OOB-written files preserves data exactly, merges files, keeps
row-ids contiguous, and leaves time-travel and re-append working.

## Examples

Runnable demos in [`examples/`](examples/) (use the `dev` group for `duckdb`):

```bash
uv run --group dev python examples/01_quickstart.py
```

Covers quickstart, source-time time-travel, late-arriving backfill, merge-on-read,
maintenance, and a Postgres-catalog variant. See [`examples/README.md`](examples/README.md).

## Object storage (S3)

Data paths can be object-store URIs (`s3://…`, including MinIO), not just local
disk. DuckDB (`httpfs`) does the Parquet read/write; `footer_and_size` reads the
footer via `fsspec`/`s3fs` — the optional **`[s3]` extra**
(`uv add "ducklake-oob-writer[s3]"`).

The package stays **auth-agnostic**: you configure credentials on the DuckDB
connection you hand it, so static keys, `PROVIDER credential_chain`, or STS
temp-creds (e.g. MinIO `AssumeRoleWithCertificate` via a `credential_process`) all
work without any package change.

```python
so = {"key": KEY, "secret": SECRET, "client_kwargs": {"endpoint_url": "http://host:9000"}}
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("CREATE SECRET (TYPE s3, KEY_ID '…', SECRET '…', "
            "ENDPOINT 'host:9000', URL_STYLE 'path', USE_SSL false)")

w.init_catalog(data_path="s3://bucket/data")
con.execute("COPY (SELECT …) TO 's3://bucket/data/main/t/batch.parquet' (FORMAT PARQUET)")
w.register_parquet("t", "s3://bucket/data/main/t/batch.parquet",
                   con=con, storage_options=so, snapshot_time=src_ts)
```

`register_data_file`/`register_parquet`/`footer_and_size`/`column_stats` take
`con` and/or `storage_options` for remote paths; local paths need neither.

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

## Concurrency

The OOB writer assumes it is the **sole writer** of a catalog — it caches the
catalog id counters (`next_snapshot_id` / `next_file_id` / `next_catalog_id`). It
does **not** participate in DuckLake's own transaction/conflict protocol; it
writes catalog rows directly. The implications (verified — see
`examples/07_concurrent_reader.py` and `08_concurrent_writers.py`):

| Scenario | SQLite catalog | Postgres catalog |
|---|---|---|
| **Readers** (any number of DuckDB attaches) while the OOB writer writes | ✅ safe — a live reader sees commits appear incrementally | ✅ safe (MVCC snapshot per reader transaction) |
| **Serialized hand-off**: native DuckDB writes, then a *fresh* OOB writer continues | ✅ works (default journal) | ✅ works |
| **Stale writer**: an external write lands between OOB operations | ✅ fails loud — `snapshot_id` PK → `IntegrityError` + rollback, **no corruption** | ✅ fails loud — `UniqueViolation` + rollback, **no corruption** |
| **Concurrent writers / WAL** | ⚠️ a SQLite catalog in **WAL** mode that DuckDB *also writes* can be **corrupted** (two SQLite implementations on one WAL file) | ✅ no file-level hazard exists — the server arbitrates all writers |

Guidance:

- **One writer per catalog.** Funnel all producers (incl. federation feeders)
  through a single OOB writer, or give each its own catalog/namespace and union on read.
- If you must hand off to/from native DuckDB, **serialize** and **re-create** the
  OOB writer afterwards (a fresh `DuckLakeWriter` re-reads the counters). A reused
  instance with stale counters fails loudly (PK rollback) rather than corrupting.
- For any shared/multi-writer setup, prefer a **Postgres catalog** — every failure
  becomes a clean transaction conflict, and the SQLite-WAL corruption hazard is gone.
- **Do not** put a SQLite catalog in WAL mode if DuckDB also writes it. (Readers
  under WAL are fine; it's mixed *writers* that corrupt.)

## Consumers

- [`rule4`](https://github.com/phrrngtn/rule4) — Socrata open-data scraping into DuckLake.
- `ha_ducklake` — tailing the Home Assistant recorder into typed marts.
