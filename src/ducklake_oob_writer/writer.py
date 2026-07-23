"""
rule4.ducklake_writer — OOB (out-of-band) writer for DuckLake metadata tables.

Maintains DuckLake catalog metadata via direct INSERT into the ducklake_*
tables using the SQLAlchemy expression API. Works across all three DuckLake
catalog backends: PostgreSQL, SQLite, and DuckDB.

DuckLake's begin_snapshot/end_snapshot is Snodgrass transaction-time
(SYSTEM_TIME): append-only snapshots, immutable history. Each mutation
(create table, add columns, register data file) allocates a new snapshot.

The writer manages two monotonic counters:
  - next_catalog_id: allocates IDs for schemas, tables, columns
  - next_file_id:    allocates IDs for data files

These are persisted in ducklake_snapshot so the writer can resume from
the last known state.

Usage:

    from rule4.ducklake_catalog import create_catalog
    from rule4.ducklake_writer import DuckLakeWriter

    engine = create_engine("postgresql://localhost/rule4_test")
    meta = create_catalog(engine, schema="ducklake")
    writer = DuckLakeWriter(engine, meta)

    # Bootstrap (once)
    writer.init_catalog(data_path="/path/to/parquet/")

    # Register a table with columns
    writer.create_table("main", "my_table",
        columns=[("col1", "varchar"), ("col2", "int64")],
        snapshot_time=some_datetime,
        commit_message="Import from source X",
    )

    # Register a data file for a table
    writer.register_data_file("my_table",
        path="data_0.parquet",
        record_count=500,
        file_size_bytes=12345,
        footer_size=678,
    )
"""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from loguru import logger
from sqlalchemy import select, func, and_, or_, literal, text

from ducklake_oob_writer import inlined
from ducklake_oob_writer.canonicalize import recanonicalize
from ducklake_oob_writer.catalog import DUCKLAKE_VERSION
from ducklake_oob_writer.incorporation import (
    INCORPORATION_LOG,
    create_incorporation_log,
    record_incorporation,
)


def _is_absolute_uri(path):
    """A fully-qualified data-file location — a URI (``s3://…``, ``gs://…``,
    ``file://…``) or an absolute filesystem path — stored verbatim with
    ``path_is_relative=False`` so a single catalog can reference files scattered across
    many backends. A bare relative path is resolved against the table directory under
    ``DATA_PATH`` (``path_is_relative=True``)."""
    return "://" in path or path.startswith("/")


class DuckLakeWriter:
    """OOB writer for DuckLake metadata tables via SA expression API.

    All writes go through the SA Table objects defined in ducklake_catalog.
    No raw SQL, no f-strings, no dialect branching.
    """

    def __init__(self, engine, meta, *, reject_out_of_order=False):
        """
        Args:
            engine: SQLAlchemy engine (PG, SQLite, or DuckDB)
            meta: MetaData from ducklake_catalog._build_metadata() or create_catalog()
            reject_out_of_order: when True, enforce **per-table transaction-time
                monotonicity** — a data/delete write whose ``snapshot_time`` precedes an
                existing snapshot for the same table is refused (``ValueError``) instead of
                being accepted and canonicalized. With no out-of-order arrivals the
                ``snapshot_id`` order can never diverge from ``snapshot_time`` order, so the
                automatic renumber becomes a guaranteed no-op (and is skipped) and a
                downstream subscriber can safely cursor on the now-stable ``snapshot_id``
                (e.g. a per-table TTST HWM). Default False preserves the accept-OOO +
                auto-canonicalize behavior (honest late-arriving backfill / federation).
        """
        self.engine = engine
        self.meta = meta
        self.reject_out_of_order = reject_out_of_order

        # Table references — keyed by unqualified name
        self._t = {}
        for key, table in meta.tables.items():
            # key may be "schema.table_name" or just "table_name"
            name = key.split(".")[-1]
            self._t[name] = table

        # Counters — loaded lazily from the latest snapshot
        self._next_catalog_id = None
        self._next_file_id = None
        self._next_snapshot_id = None
        self._schema_version = None

    # ── Out-of-order policy (per-table transaction-time monotonicity) ───
    def _reject_if_ooo(self, conn, table_id, snapshot_time):
        """When ``reject_out_of_order`` is set, refuse a data/delete write whose
        ``snapshot_time`` precedes any existing snapshot that already touched this table.

        **Per-table, not global**: independently-tailed tables may advance on different source
        clocks, so table B's legitimately-earlier arrival must not be blocked by table A.
        Attribution reuses the change-log tokens (``inserted_into_table:<tid>`` etc.), matched
        boundary-safely — the ``changes_made`` value is comma-wrapped so ``LIKE`` distinguishes
        tid ``5`` from ``50`` and handles the comma-joined form apply_commit writes."""
        if not self.reject_out_of_order or snapshot_time is None or table_id is None:
            return
        snap, changes = self._snapshot, self._snapshot_changes
        ops = ("inserted_into_table", "inlined_insert", "deleted_from_table",
               "inlined_delete", "altered_table")
        wrapped = literal(",").concat(changes.c.changes_made).concat(literal(","))
        touched = or_(*[wrapped.like(f"%,{op}:{table_id},%") for op in ops])
        later = conn.execute(
            select(func.count()).select_from(
                snap.join(changes, changes.c.snapshot_id == snap.c.snapshot_id))
            .where(and_(touched, snap.c.snapshot_time > snapshot_time))).scalar()
        if later:
            raise ValueError(
                f"reject_out_of_order: snapshot_time {snapshot_time!r} for table_id "
                f"{table_id} precedes {later} existing snapshot(s) for that table — "
                f"out-of-order arrival refused (per-table transaction-time monotonicity)")

    def _maybe_recanonicalize(self, conn):
        """Renumber snapshots into transaction-time order — unless ``reject_out_of_order`` is
        set, in which case monotonicity is enforced at write time so the renumber is a
        guaranteed no-op; skipping it keeps ``snapshot_id`` provably stable for downstream
        cursors (and saves the scan)."""
        if self.reject_out_of_order:
            return
        recanonicalize(conn)

    # ── Table accessors ────────────────────────────────────────────────

    @property
    def _snapshot(self):
        return self._t["ducklake_snapshot"]

    @property
    def _snapshot_changes(self):
        return self._t["ducklake_snapshot_changes"]

    @property
    def _schema_versions(self):
        return self._t["ducklake_schema_versions"]

    @property
    def _schema(self):
        return self._t["ducklake_schema"]

    @property
    def _table(self):
        return self._t["ducklake_table"]

    @property
    def _column(self):
        return self._t["ducklake_column"]

    @property
    def _data_file(self):
        return self._t["ducklake_data_file"]

    @property
    def _metadata(self):
        return self._t["ducklake_metadata"]

    @property
    def _table_stats(self):
        return self._t["ducklake_table_stats"]

    @property
    def _table_column_stats(self):
        return self._t["ducklake_table_column_stats"]

    @property
    def _file_column_stats(self):
        return self._t["ducklake_file_column_stats"]

    # ── Counter management ─────────────────────────────────────────────

    def _load_state(self, conn):
        """Load current counter state from the latest snapshot."""
        if self._next_snapshot_id is not None:
            return

        snap = self._snapshot
        stmt = select(
            func.max(snap.c.snapshot_id),
            func.max(snap.c.next_catalog_id),
            func.max(snap.c.next_file_id),
            func.max(snap.c.schema_version),
        )
        row = conn.execute(stmt).fetchone()

        if row[0] is None:
            # Empty catalog — will be initialized by init_catalog()
            self._next_snapshot_id = 0
            self._next_catalog_id = 1
            self._next_file_id = 0
            self._schema_version = 0
        else:
            self._next_snapshot_id = row[0] + 1
            self._next_catalog_id = row[1]
            self._next_file_id = row[2]
            self._schema_version = row[3]

    def _alloc_catalog_id(self):
        """Allocate a catalog ID (for schemas, tables, columns)."""
        cid = self._next_catalog_id
        self._next_catalog_id += 1
        return cid

    def _alloc_file_id(self):
        """Allocate a data file ID."""
        fid = self._next_file_id
        self._next_file_id += 1
        return fid

    def _alloc_snapshot_id(self):
        """Allocate a snapshot ID."""
        sid = self._next_snapshot_id
        self._next_snapshot_id += 1
        return sid

    # ── Core operations ────────────────────────────────────────────────

    def init_catalog(self, data_path, version=DUCKLAKE_VERSION, author=None):
        """Bootstrap an empty DuckLake catalog.

        Creates snapshot 0, the 'main' schema, and required metadata entries.

        Args:
            data_path: Absolute path to the data directory (for Parquet files)
            version: DuckLake version string
            author: Optional author for the initial snapshot
        """
        # DuckLake's native reader normalizes DATA_PATH to a trailing slash and
        # compares it literally against the stored value on ATTACH; store it the
        # same way so `ATTACH ... (DATA_PATH '<dir>')` matches without OVERRIDE.
        data_path = data_path.rstrip("/") + "/"
        with self.engine.begin() as conn:
            self._load_state(conn)

            snapshot_id = self._alloc_snapshot_id()
            schema_id = self._alloc_catalog_id()

            # Metadata entries
            conn.execute(self._metadata.insert(), [
                {"key": "version", "value": version, "scope": None, "scope_id": None},
                {"key": "created_by", "value": "rule4 ducklake_writer", "scope": None, "scope_id": None},
                {"key": "data_path", "value": data_path, "scope": None, "scope_id": None},
                {"key": "encrypted", "value": "false", "scope": None, "scope_id": None},
            ])

            # Snapshot 0
            conn.execute(self._snapshot.insert().values(
                snapshot_id=snapshot_id,
                snapshot_time=func.now(),
                schema_version=0,
                next_catalog_id=self._next_catalog_id,
                next_file_id=self._next_file_id,
            ))

            # Main schema
            conn.execute(self._schema.insert().values(
                schema_id=schema_id,
                schema_uuid=str(uuid4()),
                begin_snapshot=snapshot_id,
                end_snapshot=None,
                schema_name="main",
                path="main/",
                path_is_relative=True,
            ))

            # Snapshot changes
            conn.execute(self._snapshot_changes.insert().values(
                snapshot_id=snapshot_id,
                changes_made='created_schema:"main"',
                author=author,
                commit_message=None,
                commit_extra_info=None,
            ))

            # Schema version
            conn.execute(self._schema_versions.insert().values(
                begin_snapshot=snapshot_id,
                schema_version=0,
                table_id=None,
            ))

    def _find_schema_id(self, conn, schema_name="main"):
        """Find the current schema_id for a named schema."""
        s = self._schema
        stmt = (
            select(s.c.schema_id)
            .where(and_(s.c.schema_name == schema_name, s.c.end_snapshot.is_(None)))
        )
        row = conn.execute(stmt).fetchone()
        if row is None:
            raise ValueError(f"Schema '{schema_name}' not found")
        return row[0]

    def _find_table_id(self, conn, table_name, schema_name="main"):
        """Find the current table_id for a named table."""
        t = self._table
        stmt = (
            select(t.c.table_id)
            .where(and_(
                t.c.table_name == table_name,
                t.c.end_snapshot.is_(None),
            ))
        )
        row = conn.execute(stmt).fetchone()
        if row is None:
            raise ValueError(f"Table '{schema_name}.{table_name}' not found")
        return row[0]

    def _data_path(self, conn):
        """The catalog's DATA_PATH (the data-file root) from ducklake_metadata."""
        m = self._t["ducklake_metadata"]
        return conn.execute(select(m.c.value).where(m.c.key == "data_path")).scalar()

    def create_table(self, schema_name, table_name, columns,
                     snapshot_time=None, author=None, commit_message=None,
                     commit_extra_info=None):
        """Register a new table with columns in the DuckLake catalog.

        Args:
            schema_name: Schema name (usually "main")
            table_name: Table name (e.g. Socrata resource ID)
            columns: List of (column_name, column_type) tuples.
                     column_type uses DuckLake internal names: varchar, int64, etc.
            snapshot_time: Source-authoritative timestamp (default: now)
            author: Optional author string
            commit_message: Optional commit message
            commit_extra_info: Optional JSON string with provenance

        Returns:
            dict with table_id, snapshot_id, column_ids
        """
        with self.engine.begin() as conn:
            self._load_state(conn)

            schema_id = self._find_schema_id(conn, schema_name)
            snapshot_id = self._alloc_snapshot_id()
            table_id = self._alloc_catalog_id()

            self._schema_version += 1

            # Snapshot
            conn.execute(self._snapshot.insert().values(
                snapshot_id=snapshot_id,
                snapshot_time=snapshot_time or func.now(),
                schema_version=self._schema_version,
                next_catalog_id=self._next_catalog_id + len(columns),
                next_file_id=self._next_file_id,
            ))

            # Schema version — record the table this version introduced (native
            # DuckLake sets table_id here; a NULL makes the compaction planner
            # read a NULL uint64 and crash).
            conn.execute(self._schema_versions.insert().values(
                begin_snapshot=snapshot_id,
                schema_version=self._schema_version,
                table_id=table_id,
            ))

            # Table
            conn.execute(self._table.insert().values(
                table_id=table_id,
                table_uuid=str(uuid4()),
                begin_snapshot=snapshot_id,
                end_snapshot=None,
                schema_id=schema_id,
                table_name=table_name,
                path=f"{table_name}/",
                path_is_relative=True,
            ))

            # Columns
            column_ids = []
            col_rows = []
            for i, (col_name, col_type) in enumerate(columns):
                col_id = self._alloc_catalog_id()
                column_ids.append(col_id)
                col_rows.append({
                    "column_id": col_id,
                    "begin_snapshot": snapshot_id,
                    "end_snapshot": None,
                    "table_id": table_id,
                    "column_order": i + 1,
                    "column_name": col_name,
                    "column_type": col_type,
                    "initial_default": None,
                    "default_value": None,
                    "nulls_allowed": True,
                    "parent_column": None,
                    "default_value_type": None,
                    "default_value_dialect": None,
                })

            if col_rows:
                conn.execute(self._column.insert(), col_rows)

            # Update next_catalog_id in the snapshot (columns consumed IDs)
            conn.execute(
                self._snapshot.update()
                .where(self._snapshot.c.snapshot_id == snapshot_id)
                .values(next_catalog_id=self._next_catalog_id)
            )

            # Snapshot changes
            conn.execute(self._snapshot_changes.insert().values(
                snapshot_id=snapshot_id,
                changes_made=f'created_table:"{schema_name}"."{table_name}"',
                author=author,
                commit_message=commit_message,
                commit_extra_info=commit_extra_info,
            ))

            # Initialise per-table stats (record_count / next_row_id / file_size_bytes)
            # so files registered later can maintain it and stay compaction-ready.
            conn.execute(self._table_stats.insert().values(
                table_id=table_id, record_count=0, next_row_id=0, file_size_bytes=0))

        return {
            "table_id": table_id,
            "snapshot_id": snapshot_id,
            "column_ids": column_ids,
        }

    def add_column(self, table_name, column_name, column_type, *, schema_name="main",
                   nulls_allowed=True, snapshot_time=None, author=None, commit_message=None):
        """Evolve a table's schema by appending a column — DuckLake-native ALTER ADD COLUMN.

        A new ``ducklake_column`` row at the next ``column_order`` with ``begin_snapshot`` =
        the ALTER snapshot, whose ``schema_version`` is bumped (a new inlined data table is
        minted for that version on the next inline). Pre-existing data files / inlined rows
        read NULL for the new column — the reader resolves the column set as of each
        snapshot's schema_version. Mirrors what DuckDB writes (``altered_table:<tid>``).
        Additive only — no drops/renames. Returns column_id / snapshot_id / schema_version.
        """
        with self.engine.begin() as conn:
            self._load_state(conn)
            table_id = self._find_table_id(conn, table_name, schema_name)
            col = self._column
            max_order = conn.execute(
                select(func.max(col.c.column_order))
                .where(and_(col.c.table_id == table_id, col.c.end_snapshot.is_(None)))).scalar() or 0

            snapshot_id = self._alloc_snapshot_id()
            column_id = self._alloc_catalog_id()        # bumps next_catalog_id
            self._schema_version += 1

            conn.execute(self._snapshot.insert().values(
                snapshot_id=snapshot_id, snapshot_time=snapshot_time or func.now(),
                schema_version=self._schema_version,
                next_catalog_id=self._next_catalog_id, next_file_id=self._next_file_id))
            conn.execute(self._schema_versions.insert().values(
                begin_snapshot=snapshot_id, schema_version=self._schema_version, table_id=table_id))
            conn.execute(col.insert().values(
                column_id=column_id, begin_snapshot=snapshot_id, end_snapshot=None,
                table_id=table_id, column_order=max_order + 1, column_name=column_name,
                column_type=column_type, initial_default=None, default_value=None,
                nulls_allowed=nulls_allowed, parent_column=None,
                default_value_type=None, default_value_dialect=None))
            conn.execute(self._snapshot_changes.insert().values(
                snapshot_id=snapshot_id, changes_made=f"altered_table:{table_id}",
                author=author,
                commit_message=commit_message or f"Add column {column_name} to {table_name}",
                commit_extra_info=None))
        logger.info("added column {col} {type} to {schema}.{table} (schema_version {sv})",
                    col=column_name, type=column_type, schema=schema_name, table=table_name,
                    sv=self._schema_version)
        return {"column_id": column_id, "snapshot_id": snapshot_id,
                "schema_version": self._schema_version}

    def reconcile_columns(self, table_name, desired_columns, *, schema_name="main",
                          snapshot_time=None):
        """Make the table's columns a superset of ``desired_columns`` (a list of
        ``(name, ducklake_type)``). Columns already present (by name) are left alone;
        missing ones are appended via :meth:`add_column`. Additive only — the schema-as-data
        driver (e.g. a ``column_role`` capture of the source) supplies the desired set, and
        this is the diff→evolve step. Returns the list of column names added."""
        with self.engine.connect() as conn:
            table_id = self._find_table_id(conn, table_name, schema_name)
            existing = {r[0] for r in conn.execute(
                select(self._column.c.column_name).where(and_(
                    self._column.c.table_id == table_id,
                    self._column.c.end_snapshot.is_(None))))}
        added = [name for name, _ in desired_columns if name not in existing]
        for name, ctype in desired_columns:
            if name not in existing:
                self.add_column(table_name, name, ctype, schema_name=schema_name,
                                snapshot_time=snapshot_time)
        return added

    def set_partitioning(self, table_name, columns, schema_name="main",
                         snapshot_time=None, author=None, commit_message=None):
        """Declare partition columns for an existing table (DuckLake-native).

        Records a partition spec (``ducklake_partition_info`` +
        ``ducklake_partition_column``). Files registered into this table afterwards
        get the spec's ``partition_id`` and per-file partition values derived from
        their own constant column (see :meth:`register_data_file`) — no Parquet is
        rewritten; the partition column(s) must already exist in the table/files.

        Args:
            columns: partition keys — a list of column names (transform ``identity``)
                or ``(column_name, transform)`` tuples.

        Returns:
            dict with partition_id, snapshot_id.
        """
        norm = [(c, "identity") if isinstance(c, str) else tuple(c) for c in columns]
        with self.engine.begin() as conn:
            self._load_state(conn)
            table_id = self._find_table_id(conn, table_name, schema_name)
            snapshot_id = self._alloc_snapshot_id()

            col = self._column
            name_to_id = {
                n: i for i, n in conn.execute(
                    select(col.c.column_id, col.c.column_name)
                    .where(and_(col.c.table_id == table_id,
                                col.c.end_snapshot.is_(None)))
                ).fetchall()
            }

            # partition_id is its own id space (independent of catalog/file ids).
            pinfo = self._t["ducklake_partition_info"]
            max_pid = conn.execute(select(func.max(pinfo.c.partition_id))).scalar()
            partition_id = (max_pid + 1) if max_pid is not None else 1

            conn.execute(self._snapshot.insert().values(
                snapshot_id=snapshot_id,
                snapshot_time=snapshot_time or func.now(),
                schema_version=self._schema_version,
                next_catalog_id=self._next_catalog_id,
                next_file_id=self._next_file_id,
            ))
            conn.execute(pinfo.insert().values(
                partition_id=partition_id, table_id=table_id,
                begin_snapshot=snapshot_id, end_snapshot=None))

            pcol = self._t["ducklake_partition_column"]
            rows = []
            for idx, (cname, transform) in enumerate(norm):
                cid = name_to_id.get(cname)
                if cid is None:
                    raise ValueError(
                        f"set_partitioning: column '{cname}' not found in "
                        f"'{schema_name}.{table_name}'")
                rows.append(dict(
                    partition_id=partition_id, table_id=table_id,
                    partition_key_index=idx, column_id=cid, transform=transform))
            conn.execute(pcol.insert(), rows)

            conn.execute(self._snapshot_changes.insert().values(
                snapshot_id=snapshot_id,
                changes_made=f'set_partition_key:"{schema_name}"."{table_name}"',
                author=author, commit_message=commit_message, commit_extra_info=None))

        return {"partition_id": partition_id, "snapshot_id": snapshot_id}

    def register_data_file(self, table_name, path, record_count,
                           file_size_bytes, footer_size,
                           snapshot_time=None, author=None,
                           commit_message=None, schema_name="main",
                           column_stats=None, content_hash=None, source_uri=None):
        """Register a Parquet data file for an existing table.

        Maintains ``ducklake_table_stats`` (record_count / next_row_id /
        file_size_bytes) and assigns a contiguous ``row_id_start`` so the catalog
        stays consistent for DuckLake's planner.

        Args:
            table_name: Table to register the file for
            path: Relative path to the Parquet file (within table directory)
            record_count: Number of rows in the file
            file_size_bytes: File size in bytes
            footer_size: Parquet footer size (last 4 bytes before PAR1 magic)
            snapshot_time: Source-authoritative timestamp (default: now)
            author: Optional author string
            commit_message: Optional commit message
            schema_name: Schema name (default "main")
            column_stats: Optional per-column statistics to populate
                ``ducklake_file_column_stats``, which makes the file
                **compaction-ready**. Either a dict ``{column_name: stats}`` or a
                list ``[(column_name, stats), ...]``; each ``stats`` is the dict
                returned by :func:`ducklake_oob_writer.parquet.column_stats`
                (value_count, null_count, column_size_bytes, min_value, max_value,
                contains_nan). Without it, the file is queryable but not compactable.

        Returns:
            dict with data_file_id, snapshot_id, row_id_start
        """
        with self.engine.begin() as conn:
            self._load_state(conn)

            table_id = self._find_table_id(conn, table_name, schema_name)

            # Content-addressed idempotence: if this exact file — same table, same
            # relative path, same content hash — is already incorporated, do nothing
            # and return its existing data_file_id. Path-aware on purpose: two hive
            # partitions can have byte-identical files at different paths.
            if content_hash is not None:
                create_incorporation_log(conn)
                df, ilog = self._data_file, INCORPORATION_LOG
                existing = conn.execute(
                    select(df.c.data_file_id)
                    .select_from(df.join(ilog, ilog.c.data_file_id == df.c.data_file_id))
                    .where(and_(df.c.table_id == table_id, df.c.path == path,
                                ilog.c.content_hash == content_hash,
                                df.c.end_snapshot.is_(None)))).scalar()
                if existing is not None:
                    logger.info(
                        "skipping already-incorporated {schema_name}.{table_name}/{path} "
                        "(content {digest}…)",
                        schema_name=schema_name, table_name=table_name, path=path,
                        digest=content_hash[:12])
                    return {"data_file_id": existing, "snapshot_id": None,
                            "row_id_start": None, "deduped": True}

            self._reject_if_ooo(conn, table_id, snapshot_time)
            snapshot_id = self._alloc_snapshot_id()
            file_id = self._alloc_file_id()

            # Read (and later advance) per-table stats. row_id_start is the running
            # next_row_id so row-id ranges are contiguous across files.
            ts = self._table_stats
            ts_row = conn.execute(
                select(ts.c.record_count, ts.c.next_row_id, ts.c.file_size_bytes)
                .where(ts.c.table_id == table_id)
            ).fetchone()
            cur_rc, cur_next, cur_size = ts_row if ts_row else (0, 0, 0)
            row_id_start = cur_next

            # Active partition spec for this table (None when unpartitioned).
            pinfo = self._t["ducklake_partition_info"]
            pcol = self._t["ducklake_partition_column"]
            spec_row = conn.execute(
                select(pinfo.c.partition_id).where(and_(
                    pinfo.c.table_id == table_id, pinfo.c.end_snapshot.is_(None)))
            ).fetchone()
            partition_id = spec_row[0] if spec_row else None

            # Snapshot
            conn.execute(self._snapshot.insert().values(
                snapshot_id=snapshot_id,
                snapshot_time=snapshot_time or func.now(),
                schema_version=self._schema_version,
                next_catalog_id=self._next_catalog_id,
                next_file_id=self._next_file_id,
            ))

            # Data file
            conn.execute(self._data_file.insert().values(
                data_file_id=file_id,
                table_id=table_id,
                begin_snapshot=snapshot_id,
                end_snapshot=None,
                file_order=None,
                path=path,
                path_is_relative=not _is_absolute_uri(path),
                file_format="parquet",
                record_count=record_count,
                file_size_bytes=file_size_bytes,
                footer_size=footer_size,
                row_id_start=row_id_start,
                partition_id=partition_id,
                encryption_key=None,
                mapping_id=None,
                partial_max=None,
            ))

            # Advance table stats
            new_vals = dict(record_count=cur_rc + record_count,
                            next_row_id=cur_next + record_count,
                            file_size_bytes=cur_size + file_size_bytes)
            if ts_row:
                conn.execute(ts.update().where(ts.c.table_id == table_id).values(**new_vals))
            else:
                conn.execute(ts.insert().values(table_id=table_id, **new_vals))

            # Per-file column stats (optional; makes the file compaction-ready)
            if column_stats:
                items = (list(column_stats.items())
                         if isinstance(column_stats, dict) else list(column_stats))
                col = self._column
                name_to_id = {
                    n: i for i, n in conn.execute(
                        select(col.c.column_id, col.c.column_name)
                        .where(and_(col.c.table_id == table_id,
                                    col.c.end_snapshot.is_(None)))
                    ).fetchall()
                }
                rows = []
                for cname, st in items:
                    cid = name_to_id.get(cname)
                    if cid is None:
                        continue
                    rows.append(dict(
                        data_file_id=file_id, table_id=table_id, column_id=cid,
                        column_size_bytes=st.get("column_size_bytes"),
                        value_count=st.get("value_count"),
                        null_count=st.get("null_count"),
                        min_value=st.get("min_value"),
                        max_value=st.get("max_value"),
                        contains_nan=st.get("contains_nan"),
                        extra_stats=None,
                    ))
                if rows:
                    conn.execute(self._file_column_stats.insert(), rows)

            # Per-file partition values (when the table is partitioned). Derive each
            # key's value from the file's own constant column — its min==max in the
            # stats — so no parquet is rewritten; the partition column must be present.
            if partition_id is not None:
                col = self._column
                id_to_name = {
                    i: n for i, n in conn.execute(
                        select(col.c.column_id, col.c.column_name)
                        .where(and_(col.c.table_id == table_id,
                                    col.c.end_snapshot.is_(None)))
                    ).fetchall()
                }
                stats_by_name = {}
                if column_stats:
                    items = (list(column_stats.items())
                             if isinstance(column_stats, dict) else list(column_stats))
                    stats_by_name = {n: s for n, s in items}
                keys = conn.execute(
                    select(pcol.c.partition_key_index, pcol.c.column_id)
                    .where(and_(pcol.c.partition_id == partition_id,
                                pcol.c.table_id == table_id))
                    .order_by(pcol.c.partition_key_index)
                ).fetchall()
                pv_rows = []
                for key_index, cid in keys:
                    cname = id_to_name.get(cid)
                    st = stats_by_name.get(cname)
                    if st is None:
                        raise ValueError(
                            f"register_data_file: '{table_name}' is partitioned by "
                            f"'{cname}' but no column stats were supplied to derive its "
                            f"value — pass column_stats (or use register_parquet)")
                    mn, mx = st.get("min_value"), st.get("max_value")
                    if mn != mx:
                        raise ValueError(
                            f"register_data_file: partition column '{cname}' is not "
                            f"constant in '{path}' (min={mn!r}, max={mx!r}); a file must "
                            f"belong to exactly one partition")
                    pv_rows.append(dict(
                        data_file_id=file_id, table_id=table_id,
                        partition_key_index=key_index,
                        partition_value=None if mn is None else str(mn)))
                if pv_rows:
                    conn.execute(self._t["ducklake_file_partition_value"].insert(), pv_rows)

            # Snapshot changes
            conn.execute(self._snapshot_changes.insert().values(
                snapshot_id=snapshot_id,
                changes_made=f"inserted_into_table:{table_id}",
                author=author,
                commit_message=commit_message or f"Register data file for {table_name}",
                commit_extra_info=None,
            ))

            # Keep snapshot_id order aligned with snapshot_time order, in THIS same
            # transaction. A backfilled (out-of-order) file would otherwise corrupt
            # AT(TIMESTAMP) time-travel; this renumbers it into place. Idempotent — a
            # Record the incorporation (both clocks + content hash) in our ancillary
            # log, in the same transaction, when a content hash is supplied.
            if content_hash is not None:
                record_incorporation(
                    conn, content_hash=content_hash, source_uri=source_uri,
                    transaction_time=(snapshot_time if snapshot_time is not None
                                      else datetime.now(timezone.utc)),
                    data_file_id=file_id, schema_name=schema_name, table_name=table_name)

            # single query and a no-op unless this file arrived out of order — so
            # callers never have to think about it (see canonicalize.py).
            self._maybe_recanonicalize(conn)

        return {"data_file_id": file_id, "snapshot_id": snapshot_id,
                "row_id_start": row_id_start, "deduped": False}

    def register_parquet(self, table_name, fs_path, *, rel_path=None,
                         snapshot_time=None, author=None, commit_message=None,
                         schema_name="main", with_column_stats=True,
                         con=None, storage_options=None, source_uri=None):
        """Register a Parquet file, computing footer size, file size, record count,
        and per-column (query-pruning) statistics from the file itself.

        Requires the ``duckdb`` package (the stats read the file). For the
        dependency-free path, compute these yourself and call
        :meth:`register_data_file` directly.

        Works for local paths and object-store URIs (``s3://…``). For remote paths:
          * pass ``con`` — a DuckDB connection that already has ``httpfs`` loaded
            and the S3 secret created (used to read record count + column stats);
          * pass ``storage_options`` — an fsspec config used by ``footer_and_size``
            (e.g. MinIO: ``{"key": …, "secret": …, "client_kwargs":
            {"endpoint_url": "http://host:9000"}}``). Needs the ``[s3]`` extra.

        Args:
            fs_path: path to the Parquet file to read (local or ``s3://…``).
            rel_path: path recorded in the catalog, relative to the table dir;
                defaults to the file's basename (the standard one-file-per-table-dir
                layout).
            with_column_stats: populate ``file_column_stats`` (query pruning).
        """
        import os

        from ducklake_oob_writer.parquet import column_stats as _column_stats
        from ducklake_oob_writer.parquet import content_hash, footer_and_size

        file_size_bytes, footer_size = footer_and_size(fs_path, storage_options=storage_options)
        chash = content_hash(fs_path, storage_options=storage_options)
        if with_column_stats:
            record_count, cstats = _column_stats(fs_path, con=con)
        else:
            from ducklake_oob_writer.parquet import duck_engine
            eng = None if con is not None else duck_engine()
            c = con or eng.connect()
            record_count = c.execute(
                text("SELECT count(*) FROM read_parquet(:p)"), {"p": fs_path}).scalar()
            if eng is not None:
                c.close()
                eng.dispose()
            cstats = None
        return self.register_data_file(
            table_name,
            path=rel_path or (fs_path if _is_absolute_uri(fs_path) else os.path.basename(fs_path)),
            record_count=record_count, file_size_bytes=file_size_bytes,
            footer_size=footer_size, snapshot_time=snapshot_time, author=author,
            commit_message=commit_message, schema_name=schema_name, column_stats=cstats,
            content_hash=chash, source_uri=source_uri)

    def register_virtual(self, table_name, fs_path, *, rel_path=None, snapshot_time=None,
                         author=None, commit_message=None, schema_name="main", con=None,
                         source_uri=None):
        """Register a hive-laid-out Parquet file whose partition columns live in the
        PATH, not the bytes (the "relocate" style — the file is left byte-identical).

        ``rel_path`` (relative to the table directory; default the basename) must be
        hive-encoded — e.g. ``country=US/part-0.parquet``. The file's own columns are
        mapped by name; each ``key=value`` segment becomes a virtual column that the
        native reader materializes *from the path* and prunes on (via min==max stats).
        The virtual columns must already exist in the table; columns that aren't
        virtual must be physically present in the file. No Parquet is rewritten.

        Requires the ``duckdb`` package (reads the real columns' stats); pass ``con``
        for an object-store file (httpfs + secret already set up).
        """
        import os

        from ducklake_oob_writer.parquet import column_stats, content_hash, footer_and_size

        rel = rel_path or (fs_path if _is_absolute_uri(fs_path) else os.path.basename(fs_path))
        hive = dict(seg.split("=", 1) for seg in os.path.dirname(rel).split("/")
                    if "=" in seg)
        file_size_bytes, footer_size = footer_and_size(fs_path)
        chash = content_hash(fs_path)
        record_count, cstats = column_stats(fs_path, con=con)
        cstats = dict(cstats) if isinstance(cstats, dict) else dict(cstats)
        for name, value in hive.items():               # virtual cols: min==max == path value
            cstats[name] = {"min_value": value, "max_value": value,
                            "value_count": record_count, "null_count": 0,
                            "column_size_bytes": 0, "contains_nan": False}

        info = self.register_data_file(
            table_name, path=rel, record_count=record_count,
            file_size_bytes=file_size_bytes, footer_size=footer_size,
            snapshot_time=snapshot_time, author=author,
            commit_message=commit_message, schema_name=schema_name, column_stats=cstats,
            content_hash=chash, source_uri=source_uri)

        if info.get("deduped"):
            return info        # already incorporated at this path — mapping is already there

        # Attach a map_by_name mapping: real columns by name; hive columns flagged
        # is_partition so DuckLake reads them from the path, not the file.
        with self.engine.begin() as conn:
            table_id = self._find_table_id(conn, table_name, schema_name)
            col = self._column
            name_to_id = {n: i for i, n in conn.execute(
                select(col.c.column_id, col.c.column_name)
                .where(and_(col.c.table_id == table_id, col.c.end_snapshot.is_(None)))
                .order_by(col.c.column_order)).fetchall()}
            cmap = self._t["ducklake_column_mapping"]
            nmap = self._t["ducklake_name_mapping"]
            max_mid = conn.execute(select(func.max(cmap.c.mapping_id))).scalar()
            mapping_id = (max_mid + 1) if max_mid is not None else 0
            conn.execute(cmap.insert().values(
                mapping_id=mapping_id, table_id=table_id, type="map_by_name"))
            rows, idx = [], 0
            for name in [n for n in name_to_id if n not in hive] + list(hive):
                rows.append(dict(mapping_id=mapping_id, column_id=idx, source_name=name,
                                 target_field_id=name_to_id[name], parent_column=None,
                                 is_partition=name in hive))
                idx += 1
            conn.execute(nmap.insert(), rows)
            conn.execute(self._data_file.update()
                         .where(self._data_file.c.data_file_id == info["data_file_id"])
                         .values(mapping_id=mapping_id))
        return {**info, "mapping_id": mapping_id, "virtual": hive}

    def inline_rows(self, table_name, rows, *, schema_name="main", snapshot_time=None,
                    author=None, commit_message=None):
        """Write rows straight into the catalog as DuckLake **inlined data** — no
        Parquet, no DuckDB, no object store. Each row lands in
        ``ducklake_inlined_data_<table_id>_<schema_version>`` in the native inlining
        format (MVCC bookkeeping ``row_id`` / ``begin_snapshot`` / ``end_snapshot`` plus
        the table's columns), and reads union it with the table's Parquet files. Ideal
        for small / frequent arrivals (e.g. CDC/CT polling since a HWM); squish to
        Parquet later with the native ``ducklake_flush_inlined_data``.

        Scalar / simple-typed columns only (int, double, varchar, boolean, decimal,
        date/time/timestamp). A nested column (LIST/STRUCT/MAP) raises — route those
        through Parquet, where types are exact and portable. ``rows`` is an iterable of
        ``{column_name: value}`` dicts.
        """
        rows = list(rows)
        if not rows:
            return {"inlined": 0, "snapshot_id": None}
        with self.engine.begin() as conn:
            self._load_state(conn)
            table_id = self._find_table_id(conn, table_name, schema_name)
            col = self._column
            cols = conn.execute(
                select(col.c.column_name, col.c.column_type)
                .where(and_(col.c.table_id == table_id, col.c.end_snapshot.is_(None)))
                .order_by(col.c.column_order)).fetchall()
            for cname, ctype in cols:
                if inlined.is_nested(ctype):
                    raise ValueError(
                        f"inline_rows: column '{cname}' has nested type '{ctype}'; inline "
                        f"only scalar/simple types — register such a table as Parquet")
            sv = self._schema_version
            inlined_name = f"ducklake_inlined_data_{table_id}_{sv}"

            # Registry + per-table inlined table as SQLAlchemy Core objects (the extension
            # makes these on first inline; for a pure-OOB lake we create them).
            inlined.REGISTRY.create(conn, checkfirst=True)
            itbl = inlined.data_table(inlined_name, cols)
            itbl.create(conn, checkfirst=True)
            reg = inlined.REGISTRY
            if not conn.execute(select(func.count()).select_from(reg)
                                .where(reg.c.table_name == inlined_name)).scalar():
                conn.execute(reg.insert().values(
                    table_id=table_id, table_name=inlined_name, schema_version=sv))

            self._reject_if_ooo(conn, table_id, snapshot_time)
            snapshot_id = self._alloc_snapshot_id()
            ts = self._table_stats
            ts_row = conn.execute(select(ts.c.record_count, ts.c.next_row_id, ts.c.file_size_bytes)
                                  .where(ts.c.table_id == table_id)).fetchone()
            cur_rc, cur_next, cur_size = ts_row if ts_row else (0, 0, 0)

            conn.execute(self._snapshot.insert().values(
                snapshot_id=snapshot_id, snapshot_time=snapshot_time or func.now(),
                schema_version=sv, next_catalog_id=self._next_catalog_id,
                next_file_id=self._next_file_id))

            payload = [{"row_id": cur_next + i, "begin_snapshot": snapshot_id, "end_snapshot": None,
                        **{cn: inlined.value(ct, r.get(cn)) for cn, ct in cols}}
                       for i, r in enumerate(rows)]
            conn.execute(itbl.insert(), payload)

            n = len(rows)
            new_ts = dict(record_count=cur_rc + n, next_row_id=cur_next + n, file_size_bytes=cur_size)
            if ts_row:
                conn.execute(ts.update().where(ts.c.table_id == table_id).values(**new_ts))
            else:
                conn.execute(ts.insert().values(table_id=table_id, **new_ts))

            conn.execute(self._snapshot_changes.insert().values(
                snapshot_id=snapshot_id, changes_made=f"inlined_insert:{table_id}",
                author=author,
                commit_message=commit_message or f"Inline {n} row(s) into {table_name}",
                commit_extra_info=None))
            self._maybe_recanonicalize(conn)
        logger.debug("inlined {n} row(s) into {schema}.{table}",
                     n=n, schema=schema_name, table=table_name)
        return {"inlined": n, "snapshot_id": snapshot_id}

    def delete_rows(self, table_name, rel_path, positions, *, schema_name="main",
                    snapshot_time=None, author=None, commit_message=None):
        """Delete specific 0-based row **positions** from a registered Parquet data file
        via a DuckLake **position delete-file** (merge-on-read) — the data file is left
        untouched. An UPDATE is delete-these-positions + ``register_*`` the new rows.

        **Out-of-order deletes are refused.** A delete references prior state (a data
        file's positions), so reordering it is unsafe; if ``snapshot_time`` precedes the
        catalog's current data frontier it raises. Tailing CT/CDC/backlog sources are
        monotonic, so this Just Works; backfilled deletes are a deliberate error.

        Needs ``duckdb`` (writes the delete-file). ``positions`` is an iterable of ints.
        """
        from uuid import uuid4
        import os
        from ducklake_oob_writer.parquet import footer_and_size, write_rows_parquet

        positions = sorted({int(p) for p in positions})
        if not positions:
            return {"deleted": 0, "snapshot_id": None}
        with self.engine.begin() as conn:
            self._load_state(conn)
            table_id = self._find_table_id(conn, table_name, schema_name)
            df = self._data_file
            row = conn.execute(select(df.c.data_file_id).where(and_(
                df.c.table_id == table_id, df.c.path == rel_path,
                df.c.end_snapshot.is_(None)))).fetchone()
            if row is None:
                raise ValueError(f"delete_rows: no current data file {rel_path!r} in "
                                 f"{schema_name}.{table_name}")
            data_file_id = row[0]

            # Refuse a backfilled (out-of-order) delete: any data snapshot dated later
            # than this delete means we'd have to reorder it — which we don't do.
            sc = self._snapshot_changes
            data_changes = (sc.c.changes_made.like("inserted_into_table:%")
                            | sc.c.changes_made.like("inlined_insert:%")
                            | sc.c.changes_made.like("deleted_from_table:%")
                            | sc.c.changes_made.like("inlined_delete:%"))
            data_sids = select(sc.c.snapshot_id).where(data_changes)
            t_del = snapshot_time or datetime.now()
            later = conn.execute(select(func.count()).select_from(self._snapshot).where(and_(
                self._snapshot.c.snapshot_id.in_(data_sids),
                self._snapshot.c.snapshot_time > t_del))).scalar()
            if later:
                raise ValueError(
                    f"out-of-order delete: snapshot_time {t_del} precedes the current data "
                    f"frontier — I'm afraid I can't do that. Deletes reference prior state, "
                    f"so they must arrive in transaction-time order (tailing CT/CDC is "
                    f"monotonic, so this normally Just Works).")

            data_path = self._data_path(conn)
            tdir = os.path.join(data_path, schema_name, table_name)
            del_rel = f"{os.path.splitext(rel_path)[0]}-delete-{uuid4().hex[:8]}.parquet"
            del_abs = os.path.join(tdir, del_rel)
            abs_data = os.path.join(tdir, rel_path)
            write_rows_parquet([("file_path", "varchar"), ("pos", "int64")],
                               [(abs_data, p) for p in positions], del_abs)
            size, footer = footer_and_size(del_abs)

            snapshot_id = self._alloc_snapshot_id()
            delete_file_id = self._alloc_file_id()   # delete files share the data-file id counter
            ts = self._table_stats
            ts_row = conn.execute(select(ts.c.record_count).where(ts.c.table_id == table_id)).fetchone()
            conn.execute(self._snapshot.insert().values(
                snapshot_id=snapshot_id, snapshot_time=snapshot_time or func.now(),
                schema_version=self._schema_version, next_catalog_id=self._next_catalog_id,
                next_file_id=self._next_file_id))
            conn.execute(self._t["ducklake_delete_file"].insert().values(
                delete_file_id=delete_file_id, table_id=table_id, begin_snapshot=snapshot_id,
                end_snapshot=None, data_file_id=data_file_id, path=del_rel, path_is_relative=True,
                format="parquet", delete_count=len(positions), file_size_bytes=size,
                footer_size=footer, encryption_key=None, partial_max=None))
            if ts_row:
                conn.execute(ts.update().where(ts.c.table_id == table_id)
                             .values(record_count=max(0, ts_row[0] - len(positions))))
            conn.execute(sc.insert().values(
                snapshot_id=snapshot_id, changes_made=f"deleted_from_table:{table_id}",
                author=author, commit_message=commit_message or
                f"Delete {len(positions)} row(s) from {table_name}", commit_extra_info=None))
            self._maybe_recanonicalize(conn)
        logger.info("deleted {n} row(s) from {schema}.{table} ({path})",
                    n=len(positions), schema=schema_name, table=table_name, path=rel_path)
        return {"deleted": len(positions), "snapshot_id": snapshot_id, "delete_file_id": delete_file_id}

    def delete_where(self, table_name, rel_path, predicate, *, schema_name="main",
                     fs_path=None, con=None, **kw):
        """Delete rows of a registered Parquet file matching a SQL ``predicate`` over the
        file's columns — resolves their physical positions with DuckDB's
        ``file_row_number`` and calls :meth:`delete_rows`. ``predicate`` is a trusted SQL
        boolean expression. Needs ``duckdb``."""
        import os
        from ducklake_oob_writer.parquet import duck_engine
        with self.engine.connect() as conn:
            data_path = self._data_path(conn)
        abs_data = fs_path or os.path.join(data_path, schema_name, table_name, rel_path)
        # read_parquet + file_row_number + the trusted predicate are duckdb-specific -> text()
        stmt = text(f"SELECT file_row_number FROM read_parquet(:p, file_row_number=true) "
                    f"WHERE {predicate}")
        eng = None if con is not None else duck_engine()
        c = con or eng.connect()
        positions = [r[0] for r in c.execute(stmt, {"p": abs_data}).fetchall()]
        if eng is not None:
            c.close()
            eng.dispose()
        return self.delete_rows(table_name, rel_path, positions, schema_name=schema_name, **kw)

    def current_tables(self):
        """List all current (non-deleted) tables.

        Returns list of dicts with table_id, table_name, begin_snapshot.
        """
        t = self._table
        stmt = (
            select(t.c.table_id, t.c.table_name, t.c.begin_snapshot)
            .where(t.c.end_snapshot.is_(None))
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return [
            {"table_id": r[0], "table_name": r[1], "begin_snapshot": r[2]}
            for r in rows
        ]

    def current_columns(self, table_name):
        """List current columns for a table.

        Returns list of dicts with column_id, column_name, column_type,
        column_order.
        """
        c = self._column
        t = self._table
        with self.engine.connect() as conn:
            table_id = self._find_table_id(conn, table_name)
            stmt = (
                select(c.c.column_id, c.c.column_name, c.c.column_type,
                       c.c.column_order)
                .where(and_(c.c.table_id == table_id, c.c.end_snapshot.is_(None)))
                .order_by(c.c.column_order)
            )
            rows = conn.execute(stmt).fetchall()
        return [
            {"column_id": r[0], "column_name": r[1], "column_type": r[2],
             "column_order": r[3]}
            for r in rows
        ]

    def snapshots(self):
        """List all snapshots with their changes.

        Returns list of dicts with snapshot_id, snapshot_time,
        changes_made, author, commit_message.
        """
        s = self._snapshot
        sc = self._snapshot_changes
        stmt = (
            select(
                s.c.snapshot_id,
                s.c.snapshot_time,
                sc.c.changes_made,
                sc.c.author,
                sc.c.commit_message,
            )
            .outerjoin(sc, s.c.snapshot_id == sc.c.snapshot_id)
            .order_by(s.c.snapshot_id)
        )
        with self.engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()
        return [
            {"snapshot_id": r[0], "snapshot_time": r[1], "changes_made": r[2],
             "author": r[3], "commit_message": r[4]}
            for r in rows
        ]
