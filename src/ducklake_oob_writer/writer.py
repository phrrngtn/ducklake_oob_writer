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

from sqlalchemy import select, func, and_

from ducklake_oob_writer.catalog import DUCKLAKE_VERSION


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

            # Schema version
            conn.execute(self._schema_versions.insert().values(
                begin_snapshot=snapshot_id,
                schema_version=self._schema_version,
                table_id=None,
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

        return {
            "table_id": table_id,
            "snapshot_id": snapshot_id,
            "column_ids": column_ids,
        }

    def register_data_file(self, table_name, path, record_count,
                           file_size_bytes, footer_size,
                           snapshot_time=None, author=None,
                           commit_message=None, schema_name="main"):
        """Register a Parquet data file for an existing table.

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

        Returns:
            dict with data_file_id, snapshot_id
        """
        with self.engine.begin() as conn:
            self._load_state(conn)

            table_id = self._find_table_id(conn, table_name, schema_name)
            snapshot_id = self._alloc_snapshot_id()
            file_id = self._alloc_file_id()

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
                path_is_relative=True,
                file_format="parquet",
                record_count=record_count,
                file_size_bytes=file_size_bytes,
                footer_size=footer_size,
                row_id_start=0,
                partition_id=None,
                encryption_key=None,
                mapping_id=None,
                partial_max=None,
            ))

            # Snapshot changes
            conn.execute(self._snapshot_changes.insert().values(
                snapshot_id=snapshot_id,
                changes_made=f"inserted_into_table:{table_id}",
                author=author,
                commit_message=commit_message or f"Register data file for {table_name}",
                commit_extra_info=None,
            ))

        return {"data_file_id": file_id, "snapshot_id": snapshot_id}

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
