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

**`recanonicalize(source_engine, target_engine)`** (implemented) fixes it: rebuild
the catalog so `snapshot_id` order matches `snapshot_time` order — the *one*
canonical arrangement. It is a pure, deterministic function of the data (sort by
`(snapshot_time, path)`, replay), touches no Parquet (reuses the catalog's stats),
and is a no-op on already-ordered input.

Because it rewrites every `snapshot_id`: **surrogates carry no meaning outside the
database and are expected to change.** Clients time-travel by `TIMESTAMP` (stable),
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

The two concerns meet in the event log: each event is *self-describing* — it records
its **encoding recipe** (relocate vs rewrite, the mapping, the stats) and its
**times** — and `recanonicalize` is just a faithful replay in `transaction_time`
order. Encoding is a property of an event; temporal order is the fold order; they
don't interact.

The two axes are now fully orthogonal in the build: `recanonicalize` replays **both**
recipes faithfully — the **rewrite** path (reuse `file_column_stats`, re-run
`register_data_file` + `set_partitioning`) *and* the **relocate/hive** path (recreate
each file's `column_mapping` / `name_mapping` / `mapping_id`, re-pointed at the rebuilt
tables' column ids). Canonicalizing a lake that contains `register_virtual` files
preserves the path-virtualized columns — proven by a native-reader round-trip after a
canonicalize (`test_canonicalize_mapping.py`).

---

## Status

| piece | state |
|---|---|
| Rewrite-style dimensions (`set_partitioning` + `min==max` derivation) | **implemented** + tested |
| `recanonicalize` (transaction-time order, replays rewrite **and** hive mappings) | **implemented** + tested |
| Relocate-style `register_virtual` (hive `name_mapping`) | **implemented** + round-trip tested (materialize + prune from the path; no rewrite) |
| Auto-trigger for canonicalization (fire on out-of-order arrival) | approach + mechanism done; only the automatic firing deferred |
| Content-hash event log + `incorporation_time`/sequence + `lake_as_known_at` | designed |
| Generalizing the fold over deletes / replacements / schema changes | open — needs per-event identity + ordering semantics |

Deletes/replacements/schema-changes are also events; the clean append/incorporate
story above assumes one artifact = one add-event, and the general case needs each
mutation to carry its own stable identity before the fold is fully general.

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

## Out-of-order handling — the decision (committed)

**Decided, and the mechanism is built.** Out-of-order / backfilled arrivals are
fixed by **synchronous full-replay `recanonicalize` + atomic swap** — explicitly
chosen *over* the async machinery in the next section. `recanonicalize` is
implemented and tested (including `register_virtual` mapping replay), and because
it builds a fresh catalog and swaps it in, the database is never observably
inconsistent. The **only** part not yet wired is firing it **automatically** on a
detected out-of-order `transaction_time`; today you invoke `recanonicalize`
explicitly after a backfill. That auto-trigger is the lone deferred temporal piece —
the *trigger*, not the *approach*.

## Deferred by design — recorded, not built (avoid over-engineering)

Recorded so the *problem* is on record, but built no further than the problem
demands, keeping the writer a small, comprehensible core:

- **Incorporation validation — "don't record crap."** *Problem:* bad inputs or
  lying stats silently corrupt query results / pruning. *Guards (when needed):* the
  Parquet actually reads and exists at the recorded path; declared columns present
  with compatible types; a partition/dimension column genuinely constant (`min==max`
  — already enforced); **content-hash dedup** so the same artifact twice is a no-op
  (idempotence); stats *derived from the file*, never trusted from a caller; a
  *plausible* `transaction_time` (the "no later than N" bound reused as a
  data-validation guard, **not** a performance watermark).
- **The auto-trigger for canonicalization** (see the committed decision above) —
  wiring `recanonicalize` to fire on its own when an arrival's `transaction_time`
  precedes an existing snapshot. The approach and mechanism are settled; only the
  automation is deferred.

**Explicitly NOT built (lily-gilding for a regime we don't have):** the append-only
event log, watermark-gated *splice* canonicalization, and derived-catalog
catch-up. At hours/days incorporation cadence over cheap metadata, the synchronous
full-replay `recanonicalize` is both simpler and always-consistent — no inconsistency
window. The guiding principle is to keep the OOB writer minimal and resist adding
machinery before a real problem motivates it.
