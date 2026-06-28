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
from sqlalchemy import select, func, and_, text

from ducklake_oob_writer.canonicalize import recanonicalize
from ducklake_oob_writer.catalog import DUCKLAKE_VERSION
from ducklake_oob_writer.incorporation import create_incorporation_log, record_incorporation


def _is_absolute_uri(path):
    """A fully-qualified data-file location — a URI (``s3://…``, ``gs://…``,
    ``file://…``) or an absolute filesystem path — stored verbatim with
    ``path_is_relative=False`` so a single catalog can reference files scattered across
    many backends. A bare relative path is resolved against the table directory under
    ``DATA_PATH`` (``path_is_relative=True``)."""
    return "://" in path or path.startswith("/")


# DuckLake scalar types the metadata backend stores natively in an inlined data table
# (keys are the uppercased DuckLake/DuckDB type names — int widths all land in BIGINT).
_INLINE_NATIVE = {
    "BIGINT": "BIGINT", "INTEGER": "BIGINT", "INT": "BIGINT", "SMALLINT": "BIGINT",
    "TINYINT": "BIGINT", "HUGEINT": "BIGINT",
    "INT8": "BIGINT", "INT16": "BIGINT", "INT32": "BIGINT", "INT64": "BIGINT",
    "UINT8": "BIGINT", "UINT16": "BIGINT", "UINT32": "BIGINT", "UINT64": "BIGINT",
    "UBIGINT": "BIGINT", "UINTEGER": "BIGINT", "USMALLINT": "BIGINT", "UTINYINT": "BIGINT",
    "DOUBLE": "DOUBLE", "FLOAT": "DOUBLE", "REAL": "DOUBLE",
    "FLOAT4": "DOUBLE", "FLOAT8": "DOUBLE", "FLOAT32": "DOUBLE", "FLOAT64": "DOUBLE",
    "VARCHAR": "VARCHAR", "TEXT": "VARCHAR", "STRING": "VARCHAR",
    "BOOLEAN": "BOOLEAN", "BOOL": "BOOLEAN",
}
# Types DuckLake inlines as VARCHAR text (its own textual representation), parsed on read.
_INLINE_TEXT = ("DECIMAL", "NUMERIC", "DATE", "TIMESTAMP", "TIME")


def _ducklake_type_base(ctype):
    return ctype.upper().split("(", 1)[0].strip()


def _is_nested_type(ctype):
    u = ctype.upper()
    return u.endswith("[]") or any(t in u for t in ("STRUCT", "MAP", "UNION", "LIST"))


def _inline_storage_type(ctype):
    """Backend column type for an inlined column — native for backend-storable scalars,
    VARCHAR (DuckLake's text form) for decimal/temporal."""
    base = _ducklake_type_base(ctype)
    if base in _INLINE_NATIVE:
        return _INLINE_NATIVE[base]
    if base in _INLINE_TEXT:
        return "VARCHAR"
    raise ValueError(f"inline_rows: unsupported column type {ctype!r} — inline only "
                     f"scalar/simple types; register such a table as Parquet instead")


def _inline_value(ctype, v):
    """Format a value for its inlined column: native scalars as-is, decimal/temporal as
    their canonical text (matching DuckLake's text serialization, which `str()` yields)."""
    if v is None:
        return None
    return v if _ducklake_type_base(ctype) in _INLINE_NATIVE else str(v)


class DuckLakeWriter:
    """OOB writer for DuckLake metadata tables via SA expression API.

    All writes go through the SA Table objects defined in ducklake_catalog.
    No raw SQL, no f-strings, no dialect branching.
    """

    def __init__(self, engine, meta):
        """
        Args:
            engine: SQLAlchemy engine (PG, SQLite, or DuckDB)
            meta: MetaData from ducklake_catalog._build_metadata() or create_catalog()
        """
        self.engine = engine
        self.meta = meta

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
                existing = conn.execute(text(
                    "SELECT df.data_file_id FROM ducklake_data_file df "
                    "JOIN oob_incorporation i ON i.data_file_id = df.data_file_id "
                    "WHERE df.table_id = :t AND df.path = :p AND i.content_hash = :h "
                    "AND df.end_snapshot IS NULL"),
                    {"t": table_id, "p": path, "h": content_hash}).scalar()
                if existing is not None:
                    logger.info(
                        "skipping already-incorporated {schema_name}.{table_name}/{path} "
                        "(content {digest}…)",
                        schema_name=schema_name, table_name=table_name, path=path,
                        digest=content_hash[:12])
                    return {"data_file_id": existing, "snapshot_id": None,
                            "row_id_start": None, "deduped": True}

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
            recanonicalize(conn)

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
            import duckdb
            c = con or duckdb.connect()
            record_count = c.execute(
                "SELECT count(*) FROM read_parquet(?)", [fs_path]).fetchone()[0]
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
                if _is_nested_type(ctype):
                    raise ValueError(
                        f"inline_rows: column '{cname}' has nested type '{ctype}'; inline "
                        f"only scalar/simple types — register such a table as Parquet")
            sv = self._schema_version
            inlined = f"ducklake_inlined_data_{table_id}_{sv}"

            # The registry + per-table inlined table (the extension makes these on first
            # inline; for a pure-OOB lake we create them, matching the native shape).
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS ducklake_inlined_data_tables "
                "(table_id BIGINT, table_name VARCHAR, schema_version BIGINT)"))
            coldefs = ", ".join(f"{cn} {_inline_storage_type(ct)}" for cn, ct in cols)
            conn.execute(text(
                f"CREATE TABLE IF NOT EXISTS {inlined} "
                f"(row_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, {coldefs})"))
            if not conn.execute(text("SELECT count(*) FROM ducklake_inlined_data_tables "
                                     "WHERE table_name=:n"), {"n": inlined}).scalar():
                conn.execute(text(
                    "INSERT INTO ducklake_inlined_data_tables (table_id, table_name, schema_version) "
                    "VALUES (:t,:n,:s)"), {"t": table_id, "n": inlined, "s": sv})

            snapshot_id = self._alloc_snapshot_id()
            ts = self._table_stats
            ts_row = conn.execute(select(ts.c.record_count, ts.c.next_row_id, ts.c.file_size_bytes)
                                  .where(ts.c.table_id == table_id)).fetchone()
            cur_rc, cur_next, cur_size = ts_row if ts_row else (0, 0, 0)

            conn.execute(self._snapshot.insert().values(
                snapshot_id=snapshot_id, snapshot_time=snapshot_time or func.now(),
                schema_version=sv, next_catalog_id=self._next_catalog_id,
                next_file_id=self._next_file_id))

            colnames = [cn for cn, _ in cols]
            collist = ", ".join(["row_id", "begin_snapshot", "end_snapshot"] + colnames)
            binds = ", ".join([":row_id", ":begin_snapshot", ":end_snapshot"]
                              + [f":{c}" for c in colnames])
            payload = []
            for i, r in enumerate(rows):
                row = {"row_id": cur_next + i, "begin_snapshot": snapshot_id, "end_snapshot": None}
                for cn, ct in cols:
                    row[cn] = _inline_value(ct, r.get(cn))
                payload.append(row)
            conn.execute(text(f"INSERT INTO {inlined} ({collist}) VALUES ({binds})"), payload)

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
            recanonicalize(conn)
        logger.debug("inlined {n} row(s) into {schema}.{table}",
                     n=n, schema=schema_name, table=table_name)
        return {"inlined": n, "snapshot_id": snapshot_id}

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
