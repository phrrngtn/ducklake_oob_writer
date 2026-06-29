# Temporal & Partitioning Design

Incorporating file-based artifacts *from elsewhere* into DuckLake, out-of-band,
decomposes into **two orthogonal, composable concerns**:

1. **Dimension encoding** — *where* the non-measure (dimension) attributes live so
   DuckLake can project and prune on them: in the **path** (relocate) or in the
   **bytes** (rewrite). A per-file, spatial/schema choice.
2. **Temporal order** — *how* snapshots are arranged so time-travel is correct when
   facts arrive out of order (OOO): transaction-time **canonicalization**. A
   catalog-wide, metadata choice.

They are independent — any encoding × any temporal state — and they compose through
one structure: a **self-describing event log** of which the DuckLake catalog is a
pure fold. The rest of this note pins down each, grounded in what was verified
empirically against the native `ducklake` reader.

---

## 1. Dimension encoding: path vs rewrite

The governing fact (verified): **DuckLake prunes/projects only on values it can
obtain from the file or its path — never from catalog metadata alone.** A column
declared in the table but absent from both the file *and* a hive path reads as
`NULL`, and pruning breaks. So a path-derived dimension has exactly two homes:

### (a) Relocate — hive-style virtual column (no byte rewrite)
Keep the Parquet byte-for-byte; place it at a `key=value` path. DuckLake then
virtualizes the dimension from the path. The catalog rows that make this work
(reverse-engineered from `ducklake_add_data_files(hive_partitioning => true)`):

- `ducklake_column_mapping(type = 'map_by_name')` — map file columns to table
  columns **by name**, not by position;
- `ducklake_name_mapping` — one row per column; the path-derived ones flagged
  `is_partition = true` (read from the hive segment, not the file);
- `ducklake_file_column_stats` with `min == max ==` the path value — for pruning;
- `ducklake_data_file.mapping_id` → that mapping.

**The value is parsed from the hive path at scan time** (an `is_partition` column
with a non-hive path errors: *"should have been read from hive partitions"*). So
this style requires `key=value` directory layout, and you lose the original path
(record it as provenance). Use when you must preserve the exact bytes of an
existing artifact.

### (b) Rewrite — constant column (any layout)
Widen the Parquet with the dimension as a constant column (dictionary encoding
makes it a few bytes/file), then register normally and `set_partitioning(…)`.
DuckLake reads it from the file like any column; pruning works via the `min==max`
stats the writer derives. Use when you're already emitting fresh bytes (e.g. a SQL
result — there is no original artifact to preserve). This path is **implemented**
(`DuckLakeWriter.set_partitioning` + the `min==max` derivation in
`register_data_file`).

> Decision rule: **byte-identical preservation → relocate; non-Parquet origin →
> rewrite.** The path→fields parsing (positional `yyyy/mm/dd`, compound
> `sales_US_residential`, etc.) is the same in both; only the destination differs —
> the `key=value` path layout (a) or the bytes (b).

---

## 2. Temporal order: OOO insertion + canonicalization

DuckLake orders a table's state by the **surrogate `snapshot_id`** (insertion
order), not by `snapshot_time`. `snapshot_time` is, per the spec, just *"the
timestamp at which the snapshot was created"* — DuckLake deliberately ascribes it
no bitemporal meaning. Natively (in-band) it is `now()`; **OOB overrides it** to
carry the *source's* transaction-time.

The hazard (verified): if files are registered **out of transaction-time order** (a
backfill), `AT (TIMESTAMP)` is wrong in both directions — a backfilled fact both
leaks into earlier as-of queries and disappears from later ones — because the
backfill's high `snapshot_id` makes its cumulative state include later-dated data.

This is fixed **automatically, in the same transaction**: `register_data_file`
renumbers the snapshots so `snapshot_id` order matches `snapshot_time` order, so
simply adding files keeps time-travel safe — the caller invokes nothing. (The
standalone `recanonicalize(engine)` is also exported for manual use.)

It is *not* a replay — everything needed is already in the catalog. The shape is
**generate the work, then do it**: a SQL `ROW_NUMBER()` ranks the snapshots (schema
first, then data by `snapshot_time`); a transient `violators` table is materialised
with just the snapshots whose id changes; the non-unique `begin/end_snapshot`
**references** are remapped with set-based `UPDATE`s (any order — DuckLake's catalog
declares no foreign keys, so there's no dependency graph); and the two tables whose
*primary key* is `snapshot_id` are renumbered with an offset move off the same
`violators` table (shift the moved ids past the max, then map them down — permuting a
unique key with no transient collision). All SQLAlchemy Core, dialect-portable, one
transaction. **Idempotency falls out of the shape**: when the catalog is already
ordered the `violators` set is empty and every statement touches zero rows — no guard,
no `EXISTS`. It touches no Parquet; the native reader doesn't read `row_id_start` or
the per-snapshot allocation counters (verified by probe), so the renumber is the
snapshot-id remap alone; column/name mappings and partition specs carry no snapshot id
and survive untouched.

Because it rewrites every moved `snapshot_id`: **surrogates carry no meaning outside
the database and are expected to change.** Clients time-travel by `TIMESTAMP` (stable),
not by `VERSION` (a derived ordinal this operation recomputes). Querying by version
is still allowed — just understood as volatile.

### Two clocks (Snodgrass terminology)
- **`transaction_time`** — the *data's*, assigned by the source; goes in the
  `snapshot_time` slot; the **AS-OF** axis. Non-monotonic as written (that's the
  whole problem). *Within* a single source it is monotonic by construction; only the
  cross-source interleave is conventional.
- **`incorporation_time`** — *ours*, our wall-clock when we wrote it; a **distinct**
  clock (not an alias), and not recoverable after canonicalization renumbers the
  snapshots. It is the **cursor** axis: tailing / CDC / HWM-driven derived tables
  must cursor on it, never on `transaction_time` (a backfill lands *below* a
  `transaction_time` HWM and is missed; it gets a *fresh* incorporation stamp).

Keep `incorporation_time` (and provenance) in a **content-hash-keyed event log**,
*not* on the volatile snapshot. Then:
```
event_log              = {content_hash → source_uri, transaction_time, incorporation_*}   (source of truth)
ducklake_catalog       = canonicalize( fold( event_log ordered by transaction_time ) )    (derived view)
lake_as_known_at(K)    = canonicalize( fold( events where incorporation ≤ K ) )            (a cheap, zero-copy facet)
```
`lake_as_known_at` answers "give me the lake as we knew it last Tuesday" by
projecting a *different catalog over the same immutable Parquet* — metadata only.

### Clock caveats (acknowledge explicitly)
Wall-clocks from several places are dodgy: **out-of-order ≠ non-causal**. Split the
caveats:
- **Interpretive** (document, no correctness impact): a cross-source
  `transaction_time` sort is a *conventional* order, not a causal one. The fold is
  deterministic regardless of clock quality, so reproducibility ("can't not work")
  holds; only the *meaning* of the interleave is caveated. Tie-break on
  `(source_id, content_hash)` — reproducible but explicitly arbitrary. If two
  sources are in genuinely different clock domains, keep **per-source** AS-OF axes
  rather than pretend one global `AT TIMESTAMP` is meaningful.
- **Load-bearing** (must enforce): the cursor needs strict monotonicity, which a
  wall-clock can't promise. Cursor on a **monotonic incorporation *sequence*** (a
  counter, single source of truth); keep `incorporation_time` as a descriptive
  attribute for human "as of last Tuesday" queries. Surrogate where you need
  provable order; wall-clock attribute where you need meaning; never conflate them.

---

## 3. How they compose (and where the seam is)

The two axes are **orthogonal by construction**. Dimension encoding lives in the
data files and in `file_column_stats` / `column_mapping` / `name_mapping` / partition
specs — *none* of which carry a snapshot id. Temporal order lives in `snapshot_id`.
So `recanonicalize`, which only renumbers `snapshot_id` and the columns that reference
it, **cannot touch the encoding** — the path-virtual and rewrite columns are preserved
for free, no special handling. Proven by a native-reader round-trip after canonicalizing
a lake of `register_virtual` files (`test_canonicalize_mapping.py`).

---

## 4. Inlined data: rows in the catalog, no files

For **small, frequent arrivals** (CDC/CT polling since a HWM), a Parquet file per batch
is the small-files anti-pattern. DuckLake's answer is **inlining**: rows live directly
in the catalog DB, in `ducklake_inlined_data_<table_id>_<schema_version>` (MVCC columns
`row_id` / `begin_snapshot` / `end_snapshot` + the table's columns), registered in
`ducklake_inlined_data_tables`; the snapshot logs `inlined_insert:<table_id>`. Reads
union the inlined rows with the table's Parquet; `ducklake_flush_inlined_data` squishes
them to a Parquet file (then `ducklake_merge_adjacent_files` compacts). `inline_rows`
writes all this as **pure SQLAlchemy** — no Parquet, no DuckDB, no object store. The CDC
loop is *poll-since-HWM → inline → flush*.

**This is governed by "mimic the native writer, don't invent"** (the OOB writer is
DuckDB's write path reimplemented from outside; the DuckDB reader expects DuckDB's
format). Two consequences:

- **Type serialization.** Inlined values must fit the catalog DB's columns. In the
  normal path DuckDB owns *both* ends — it stores non-native types (decimal, timestamp,
  …) as its own `CAST(… AS VARCHAR)` text and parses it back. So OOB must reproduce that
  text. For scalar / decimal / temporal types Python `str()` reproduces it **byte-for-
  byte** — asserted by a *differential test* that writes the same rows via native DuckDB
  and via `inline_rows` and diffs the catalog (`test_inline.py`). Nested types
  (`LIST`/`STRUCT`/`MAP`) serialize to DuckDB's bespoke literal form (`[x, y]`,
  `{'a': 1}`), not safe to reproduce from outside — so `inline_rows` **refuses them**
  (route through Parquet). A DuckDB-as-coercion-oracle path could lift that limit but
  isn't worth the dependency; the restriction stands.
- **Temporal order.** An inlined insert writes *no* `ducklake_data_file` row, so
  `recanonicalize` detects data snapshots from the change log
  (`inserted_into_table:% | inlined_insert:%`), not from `ducklake_data_file`; and it
  remaps `begin/end_snapshot` in the dynamic inlined tables too. So an **out-of-order
  inlined backfill** (the CDC late arrival) self-canonicalizes and time-travels right
  (`test_inline.py::…self_canonicalizes`).

---

## Status

| piece | state |
|---|---|
| Rewrite-style dimensions (`set_partitioning` + `min==max` derivation) | **implemented** + tested |
| `recanonicalize` (in-place set-based snapshot renumber, idempotent, SA-Core/dialect-portable) | **implemented** + tested |
| Relocate-style `register_virtual` (hive `name_mapping`) | **implemented** + round-trip tested (materialize + prune from the path; no rewrite) |
| Automatic canonicalization inside `register_data_file` (runs in the write transaction) | **implemented** + tested |
| Content-hash incorporation log (`oob_incorporation`: both clocks + hash) + `lake_as_known_at` | **implemented** + tested |
| Heterogeneous file locations (absolute URIs / `path_is_relative=False`; one catalog, many backends) | **implemented** + tested |
| Inlined data (`inline_rows`: rows in the catalog, no Parquet/DuckDB) — scalar/simple types; OOO-safe; flush to Parquet natively | **implemented** + tested (differential vs native DuckDB) |
| File-resident **deletes** (`delete_rows` / `delete_where`: DuckLake position delete-files, merge-on-read) — OOO-refused | **implemented** + tested |
| **Current-state replica** (`Replica.apply(upserts, deletes)`: net-change merge for CT / CDC-as-CT) | **implemented** + tested |
| Diff-compression's delta recompute on out-of-order backfill | open — neighbour-relative; a higher (diff) layer, not the primitive |

`delete_rows` writes a `(file_path, pos)` delete-file and registers it in
`ducklake_delete_file` (the data file is untouched); an **UPDATE** is delete-positions +
`register_*` the replacement. **Out-of-order deletes are refused** by design: a delete
references prior state (a data file's positions), so reordering it is unsafe — and tailing
CT/CDC/backlog sources are monotonic, so this Just Works; a backfilled delete is a
deliberate error rather than a niche feature. (`recanonicalize` now orders delete
snapshots by transaction-time too, so an OOO *insert* arriving alongside existing deletes
still canonicalizes correctly.) The one genuinely-open piece is **diff-compression**:
synthesising deltas from periodic full snapshots makes the deltas *neighbour-relative*, so
an out-of-order backfill there forces a delta *recompute* — that lives in the diff layer,
above this primitive.

---

## Multi-tenancy: one lake per metadata schema (decided)

Many independent lakes share one catalog *backend* by giving each its own
**metadata schema** — DuckLake's native `METADATA_SCHEMA` on `ATTACH`, and the
writer's existing `create_catalog(engine, schema=…)`. Each lake stays
single-tenant and pristine; `DROP SCHEMA … CASCADE` reaps one cleanly. No
database-per-lake, no `TRUNCATE`, no superposition. This is a zero-code decision —
the mechanism already exists on both sides.

**Rejected — row-level tenancy** (widening `ducklake_*` with a `lake_id`): the
native reader issues *unqualified* `SELECT … FROM ducklake_data_file`, so it would
read every tenant's files as one mixed lake. It breaks native compatibility, and —
since you'd need per-lake filtered views to repair it — doesn't even save the
per-lake DDL it was meant to avoid.

**Deferred — a catalog-of-lakes registry** (the catalog-service / metastore
pattern; a directory *of* catalogs, which DuckLake deliberately doesn't provide).
Would own discovery, ownership, and ad-hoc-lake lifecycle (TTL + GC), scraping each
lake's `ducklake_*` (plain SQL) for a summary. Worth building only once *managing
many lakes* is an actual problem — not yet.

## Out-of-order handling — the decision (committed, built, automatic)

Out-of-order / backfilled arrivals are fixed by a **synchronous, in-place, idempotent**
renumber — chosen *over* the async machinery in the next section — and it runs
**automatically inside `register_data_file`, in the write transaction**, so the caller
invokes nothing and a broken state is never observable. It is one transaction of
set-based SQLAlchemy-Core DML (dialect-portable) that materialises the out-of-order
snapshots and renumbers them; when the arrival was in order the work-set is empty and
it's a no-op. (`recanonicalize(engine)` stays exported for manual/standalone use.)

## Deferred by design — recorded, not built (avoid over-engineering)

Recorded so the *problem* is on record, but built no further than the problem
demands, keeping the writer a small, comprehensible core:

- **Incorporation validation — "don't record crap."** *Problem:* bad inputs or
  lying stats silently corrupt query results / pruning. *Guards (when needed):* the
  Parquet actually reads and exists at the recorded path; declared columns present
  with compatible types; a partition/dimension column genuinely constant (`min==max`
  — already enforced); stats *derived from the file*, never trusted from a caller; a
  *plausible* `transaction_time` (the "no later than N" bound reused as a
  data-validation guard, **not** a performance watermark). (**Content-hash dedup** —
  same `(table, rel_path, content_hash)` twice is a no-op — is now **built**:
  `register_*` records a sha256 in `oob_incorporation` and `register_data_file`
  short-circuits a re-incorporation. Path-aware so byte-identical hive partitions at
  different paths stay distinct.)

**Explicitly NOT built (lily-gilding for a regime we don't have):** the append-only
event log, a watermark that would bound even the inversion check + recompute to a
recent window, and a derived-catalog catch-up. At hours/days incorporation cadence
over cheap metadata, the synchronous in-place `recanonicalize` is both simpler and
always-consistent — no inconsistency window, and it already writes only the moved
slice. The guiding principle is to keep the OOB writer minimal and resist adding
machinery before a real problem motivates it.
