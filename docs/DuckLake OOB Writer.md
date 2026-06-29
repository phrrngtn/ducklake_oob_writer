# DuckLake OOB Writer

`ducklake_oob_writer` is a small, standalone package (one runtime dep: SQLAlchemy)
that registers **externally-written Parquet files directly into a DuckLake
catalog** — Postgres, SQLite, or DuckDB — via the SQLAlchemy expression API,
without ever going through DuckDB's `ducklake` extension write path.

It was extracted verbatim from [[rule4]] so that rule4 (Socrata scraping) and
[[ha_ducklake]] (Home Assistant recorder replication) share **one**
implementation. See [[DuckLake]] and [[Bitemporal Data]].

## Why out-of-band? To carry the source's transaction-time

**This is the primary reason the writer exists — not merely "register external Parquet."**

A DuckLake commit creates a *snapshot*, and every snapshot has a timestamp. If
you ingest through DuckDB's native `ducklake` INSERT/COPY, that snapshot
timestamp is **the ETL process's wall-clock at load time** (`now()`). That is the
wrong clock. It records *when our replicator happened to run*, which is an
artifact of scheduling, retries, outages, and network partitions — not a fact
about the data.

The OOB writer takes a **`snapshot_time`** argument on both
`create_table(...)` and `register_data_file(...)`. We pass the
**source-authoritative transaction-time** — the timestamp the source system
itself assigns to the fact:

- Socrata: the dataset row's `:updated_at` (the platform's transaction-time).
- Home Assistant: the recorder's `last_updated_ts` (raw states) or the
  statistic interval `start_ts` (long-term statistics).

```python
writer.register_data_file(
    table_name="lts_hourly",
    path="lts_hourly/batch-0001.parquet",
    record_count=n, file_size_bytes=sz, footer_size=fs,
    snapshot_time=source_transaction_ts,   # <-- the whole point
    commit_message="ha recorder tail @ source tx-time",
)
```

The native write path gives you no hook for this; it always stamps `now()`. OOB
is how we keep the source clock.

## What this buys us: an honest bitemporal lake

With the source transaction-time on the snapshot, the DuckLake history becomes
**bitemporal**:

- **Valid/transaction-time** (from the source) lives on the snapshot timestamp
  and in the data columns themselves (e.g. the HA interval `ts`).
- **System-time** (when *we* recorded it) is still observable, because the
  catalog rows are written in insertion order and provenance is captured
  separately (below).

Concretely, this makes the following correct rather than misleading:

1. **`AS OF` time-travel reflects source reality.** "What did the source assert
   as of 2026-04-01?" answers with the source's own timeline, not with an
   accident of when our cron fired.
2. **Late-arriving / federation backfills are honest.** A Home Assistant node
   that was network-partitioned (e.g. over Tailscale) and reconnects replays old
   rows. Each replayed batch is registered with **its source transaction-time**,
   so it lands at the correct point in the lake's history — a genuine
   "blast from the past" — instead of being smeared onto the day it finally
   synced. Without OOB, every backfill would look like it happened at catch-up
   time, corrupting any time-based analysis.
3. **Reproducibility.** Re-running the replicator does not move timestamps,
   because they come from the source, not the clock.

This is the same separation argued for elsewhere: **use the (catalog) database's
ACID transactions for the metadata, and keep the data as immutable Parquet** —
and crucially, **let the *source* own the time axis**, not the ETL.

## Mechanism

1. The caller writes a Parquet file out-of-band (duckdb, pyarrow, anything).
   Data files are immutable and uniquely named — write-once, never mutated,
   never renamed. The filesystem is never a transaction coordinator.
2. The caller calls `register_data_file(...)` / `create_table(...)`, which open a
   single SQLAlchemy transaction against the catalog DB and `INSERT` rows into the
   DuckLake catalog tables — `ducklake_snapshot`, `ducklake_snapshot_changes`,
   `ducklake_table`, `ducklake_column`, `ducklake_data_file`, etc. — using the
   `MetaData` table definitions in `catalog.py` (`DUCKLAKE_METADATA`).
3. The atomic commit is the **catalog DB transaction**. DuckDB's native
   `ducklake` reader then reads the catalog + the Parquet files and sees the
   registered data exactly as if it had been written natively.

So the data plane touches only dumb immutable blobs; the one atomic step is a row
insert in a real ACID database — robust even over flaky shared filesystems
(NFS/Spectrum Scale), because no filesystem rename/exclusive-create is on the
commit path.

## Maintenance & the compaction boundary

Maintenance (compaction, expiring snapshots, GC) is **delegated to DuckLake's own
engine**, and the package draws a hard line here:

> **This package never reads, merges, or rewrites Parquet data files for
> compaction.** The data-file work is done entirely by DuckDB's native `ducklake`
> extension. `compact()` / `expire_snapshots()` / `cleanup_old_files()` each
> `ATTACH` the catalog and issue a single `CALL ducklake_*(...)`.

The package's *only* role in compaction is to **enable** it. The one thing
DuckLake's native compaction planner actually requires that a naive OOB
registration omits is a **per-table `ducklake_schema_versions` row** — so the
writer emits it (plus row-id bookkeeping) at registration time:

- **`ducklake_schema_versions.table_id`** — set to the new table by `create_table`.
  This is *the* compaction requirement (see the citation below); a NULL here makes
  the planner read a NULL `uint64` and assert.
- `ducklake_table_stats` — `record_count` / `next_row_id` / `file_size_bytes`, and
  contiguous `row_id_start` per file — the row-id bookkeeping, maintained on
  `create_table` and every `register_data_file`.
- `ducklake_file_column_stats` — per-column counts/sizes/min/max, populated when
  `column_stats` is supplied (e.g. by `register_parquet`). **For query pruning, NOT
  compaction** — see below.

Reading a Parquet file to *compute* pruning stats (inside `register_parquet`) is
metadata work, not compaction — the boundary is about never doing the data-file
*merge/rewrite* in Python. The boundary and the correctness of native compaction
over OOB-written files are enforced by `tests/test_native_compaction.py`
(exact-data preservation across types + NULLs, real file reduction, valid merged
Parquet, contiguous row-ids, time-travel, re-append, and **bare-`register_data_file`
compaction** with no column stats).

### Spec vs. implementation: the `schema_versions.table_id` invariant

Native compaction over OOB-written files needs one thing the **published catalog
schema does not declare**: every table must have a per-table
`ducklake_schema_versions` row with a non-NULL `table_id`. This is an
*implementation* invariant, not a protocol constraint. Studied from the DuckLake
extension source (`github.com/duckdb/ducklake`,
`src/storage/ducklake_metadata_manager.cpp`):

1. **The schema declares the column nullable.** The catalog DDL (~line 281):
   ```sql
   CREATE TABLE {METADATA_CATALOG}.ducklake_schema_versions(
     begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT);  -- nullable; no NOT NULL
   ```
2. **The implementation requires it non-NULL anyway.** `table_id` was added in the
   v0.3→0.4 migration (`MigrateV03`), which converts `schema_versions` from a global
   version log to per-table tracking, backfills `table_id`, then:
   ```sql
   DELETE FROM {METADATA_CATALOG}.ducklake_schema_versions WHERE table_id IS NULL;
   ```
   The extension's own migration deletes the rows the schema permits.
3. **The compaction planner reads it unchecked.** `GetFilesForCompaction`
   (~line 2207) builds `snapshot_ranges` from `ducklake_schema_versions WHERE
   table_id = <table>`, LEFT-JOINs the data files to it, and reads the resulting
   `schema_version` with no null check:
   ```cpp
   new_entry.file.row_id_start = ...;            // IsNull-checked
   new_entry.file.end_snapshot = row.IsNull(..)? // IsNull-checked
   new_entry.schema_version = row.GetValue<idx_t>(col_idx++);  // <-- NOT null-checked
   ```
   With `table_id = NULL` the LEFT JOIN yields NULL `schema_version`, and that
   unchecked `GetValue<idx_t>` is the `"GetValueInternal on a value that is NULL"`
   assert.

**Consequences:** (a) implementing the published table schemas is *necessary but
not sufficient* — an OOB writer must also satisfy this implementation invariant;
(b) `GetFilesForCompaction` reads only `schema_versions` + `ducklake_data_file`
columns, **not** the stats tables, so `column_stats`/`file_column_stats` are *not*
a compaction prerequisite (verified: bare `register_data_file` compacts). They are
purely for query pruning. (c) The catalog protocol keeps evolving — a `v1.1`
metadata manager already exists upstream — so pinning `DUCKLAKE_VERSION` and
tracking the source remains load-bearing.

## Provenance

Lineage rides on `ducklake_snapshot_changes.commit_extra_info` as a JSON string
(source system, dataset/entity id, OTel `traceparent`, service name) — see
rule4's `doc/provenance_capture.md`. The **data** rows stay raw; provenance is
metadata-level only.

## Implementation notes

### SQLAlchemy Core for the catalog; raw DuckDB for the Parquet tool

There are two distinct roles a database plays here, and only one is a portability
surface — so they get different treatment:

- **The catalog** (the `ducklake_*` metadata tables) is **polymorphic**: it may be
  PostgreSQL *or* SQLite *or* DuckDB. Every statement that touches it must compile
  against whichever backend it happens to be, so **all catalog DML uses the SQLAlchemy
  expression language with bound parameters — no embedded SQL.** This includes the
  *dynamic* inlined data tables (`ducklake_inlined_data_<tid>_<sv>`), which `inlined.py`
  exposes as runtime `Table` objects whose column types render DuckLake's native storage
  (BIGINT/DOUBLE/VARCHAR/BOOLEAN), so create/insert/update go through Core just like the
  static tables.

- **DuckDB used as a Parquet tool** (`read_parquet`, `COPY … TO`, `file_row_number`) is
  **monomorphic**: it is *always* DuckDB regardless of the catalog backend, used to read
  and write the actual data files. It is not a portability surface — there is no other
  dialect it must also satisfy — so raw `duckdb` SQL is correct there. Wrapping it in
  SQLAlchemy or a `@compiles` dialect compiler would be machinery for a portability that
  doesn't exist.

The rule: **abstract the polymorphic surface (the catalog → Core); leave the monomorphic
tool alone (DuckDB-qua-Parquet → raw).** A dialect compiler (`@compiles`) earns its keep
only when a construct must render *differently across dialects*; the file-ops render one
way, so there is nothing to compile per-dialect.

## Constraints / gotchas

- **Single writer per catalog.** Catalog id counters (`next_catalog_id`,
  `next_file_id`, `next_snapshot_id`) are read from the latest
  `ducklake_snapshot` row on every transaction; concurrent writers would
  collide. Funnel all writes (including all federation feeders) through one
  ingester, or give each source its own catalog/namespace and union on read.
- **Catalog protocol coupling.** The SA `MetaData` mirrors a specific DuckLake
  catalog version (`DUCKLAKE_VERSION`, currently `1.0`). DuckDB's `ducklake`
  extension refuses to attach a catalog stamped older than it expects. If
  DuckLake bumps its catalog *layout*, `catalog.py` must track it.

### DuckLake 1.0 interop notes (verified against DuckDB 1.5.4)

Getting an OOB-written catalog to read natively requires matching four things
the native reader expects. All are handled by the package or must be supplied by
the caller as noted:

1. **Version string.** `DUCKLAKE_VERSION = "1.0"`. The 0.4 → 1.0 bump is a pure
   version change — the catalog table layout is **column-for-column identical**
   (verified across all 27 core tables); 1.0 is just the GA stabilization.
2. **`DATA_PATH` trailing slash.** The reader normalizes `DATA_PATH` to a
   trailing slash and compares it *literally* to the stored value. `init_catalog`
   now normalizes to a trailing slash so `ATTACH ... (DATA_PATH '<dir>')` matches
   without `OVERRIDE_DATA_PATH`. (Package-handled.)
3. **Internal type names** (caller-supplied in `create_table` columns): DuckLake
   uses `varchar`, `timestamp`, `date`, `boolean`, `float64` (DOUBLE),
   `int64` (BIGINT), `int32` (INTEGER), `decimal(p,s)` — **not** `double`/`bigint`.
4. **`data_file.path` is relative to the *table* directory**, i.e. just the
   filename. The reader resolves `DATA_PATH + schema.path + table.path +
   data_file.path`; passing `main/<table>/<file>` doubles the prefix. Pass only
   `<file>.parquet`, and physically place it at
   `<DATA_PATH>/<schema>/<table>/<file>.parquet`.
- **Idempotency is merge-on-read**, not `MERGE`: append immutable Parquet, resolve
  duplicates/revisions at query time with
  `ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY <source_ts> DESC)`.

## Consumers

- [[rule4]] — Socrata open-data scraping into DuckLake.
- [[ha_ducklake]] — tailing the Home Assistant recorder into typed marts.
