"""
SQLAlchemy model for the DuckLake catalog schema (v0.4).

Defines all 29 DuckLake metadata tables, derived from the authoritative source:
  https://github.com/duckdb/ducklake/blob/main/src/storage/ducklake_metadata_manager.cpp

This allows creating the catalog schema in any SQLAlchemy-supported dialect.

Usage:
    from rule4.ducklake_catalog import create_catalog

    engine = create_engine("postgresql://localhost/ducklake_test")
    create_catalog(engine, schema="ducklake")
"""

from sqlalchemy import (
    MetaData, Table, Column,
    BigInteger, Boolean, String, Text, DateTime,
)

# DuckLake catalog protocol version. The catalog *table layout* is identical
# across 0.x and 1.0 (verified column-for-column); 1.0 is the GA stabilization.
# DuckDB's `ducklake` extension refuses to attach a catalog whose recorded
# version is older than it expects, so we stamp the current GA version.
DUCKLAKE_VERSION = "1.0"


def _build_metadata(schema=None):
    """Build SQLAlchemy MetaData with all 29 DuckLake catalog tables.

    Column types follow the DuckLake spec:
      - BIGINT → BigInteger
      - VARCHAR → Text (no length limit)
      - BOOLEAN → Boolean
      - TIMESTAMPTZ → DateTime(timezone=True)
      - UUID → String (works across DuckDB/PG/SQLite)

    Primary keys use autoincrement=False to avoid SERIAL/BIGSERIAL
    generation, since DuckLake manages IDs itself.
    """
    meta = MetaData(schema=schema)

    # ── Core metadata ────────────────────────────────────────────────

    Table("ducklake_metadata", meta,
        Column("key", Text, nullable=False, primary_key=True),
        Column("value", Text, nullable=False),
        Column("scope", Text),
        Column("scope_id", BigInteger),
    )

    Table("ducklake_snapshot", meta,
        Column("snapshot_id", BigInteger, primary_key=True, autoincrement=False),
        Column("snapshot_time", DateTime(timezone=True)),
        Column("schema_version", BigInteger),
        Column("next_catalog_id", BigInteger),
        Column("next_file_id", BigInteger),
    )

    Table("ducklake_snapshot_changes", meta,
        Column("snapshot_id", BigInteger, primary_key=True, autoincrement=False),
        Column("changes_made", Text),
        Column("author", Text),
        Column("commit_message", Text),
        Column("commit_extra_info", Text),
    )

    # ── Schema versioning ────────────────────────────────────────────

    Table("ducklake_schema_versions", meta,
        Column("begin_snapshot", BigInteger),
        Column("schema_version", BigInteger),
        Column("table_id", BigInteger),
    )

    # ── Schema / Table / View / Column ───────────────────────────────

    Table("ducklake_schema", meta,
        Column("schema_id", BigInteger, primary_key=True, autoincrement=False),
        Column("schema_uuid", String),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
        Column("schema_name", Text),
        Column("path", Text),
        Column("path_is_relative", Boolean),
    )

    Table("ducklake_table", meta,
        Column("table_id", BigInteger),
        Column("table_uuid", String),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
        Column("schema_id", BigInteger),
        Column("table_name", Text),
        Column("path", Text),
        Column("path_is_relative", Boolean),
    )

    Table("ducklake_view", meta,
        Column("view_id", BigInteger),
        Column("view_uuid", String),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
        Column("schema_id", BigInteger),
        Column("view_name", Text),
        Column("dialect", Text),
        Column("sql", Text),
        Column("column_aliases", Text),
    )

    Table("ducklake_column", meta,
        Column("column_id", BigInteger),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
        Column("table_id", BigInteger),
        Column("column_order", BigInteger),
        Column("column_name", Text),
        Column("column_type", Text),
        Column("initial_default", Text),
        Column("default_value", Text),
        Column("nulls_allowed", Boolean),
        Column("parent_column", BigInteger),
        Column("default_value_type", Text),
        Column("default_value_dialect", Text),
    )

    # ── Tags ─────────────────────────────────────────────────────────

    Table("ducklake_tag", meta,
        Column("object_id", BigInteger),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
        Column("key", Text),
        Column("value", Text),
    )

    Table("ducklake_column_tag", meta,
        Column("table_id", BigInteger),
        Column("column_id", BigInteger),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
        Column("key", Text),
        Column("value", Text),
    )

    # ── Data files ───────────────────────────────────────────────────

    Table("ducklake_data_file", meta,
        Column("data_file_id", BigInteger, primary_key=True, autoincrement=False),
        Column("table_id", BigInteger),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
        Column("file_order", BigInteger),
        Column("path", Text),
        Column("path_is_relative", Boolean),
        Column("file_format", Text),
        Column("record_count", BigInteger),
        Column("file_size_bytes", BigInteger),
        Column("footer_size", BigInteger),
        Column("row_id_start", BigInteger),
        Column("partition_id", BigInteger),
        Column("encryption_key", Text),
        Column("mapping_id", BigInteger),
        Column("partial_max", BigInteger),
    )

    Table("ducklake_delete_file", meta,
        Column("delete_file_id", BigInteger, primary_key=True, autoincrement=False),
        Column("table_id", BigInteger),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
        Column("data_file_id", BigInteger),
        Column("path", Text),
        Column("path_is_relative", Boolean),
        Column("format", Text),
        Column("delete_count", BigInteger),
        Column("file_size_bytes", BigInteger),
        Column("footer_size", BigInteger),
        Column("encryption_key", Text),
        Column("partial_max", BigInteger),
    )

    # ── File statistics ──────────────────────────────────────────────

    Table("ducklake_file_column_stats", meta,
        Column("data_file_id", BigInteger),
        Column("table_id", BigInteger),
        Column("column_id", BigInteger),
        Column("column_size_bytes", BigInteger),
        Column("value_count", BigInteger),
        Column("null_count", BigInteger),
        Column("min_value", Text),
        Column("max_value", Text),
        Column("contains_nan", Boolean),
        Column("extra_stats", Text),
    )

    Table("ducklake_file_variant_stats", meta,
        Column("data_file_id", BigInteger),
        Column("table_id", BigInteger),
        Column("column_id", BigInteger),
        Column("variant_path", Text),
        Column("shredded_type", Text),
        Column("column_size_bytes", BigInteger),
        Column("value_count", BigInteger),
        Column("null_count", BigInteger),
        Column("min_value", Text),
        Column("max_value", Text),
        Column("contains_nan", Boolean),
        Column("extra_stats", Text),
    )

    Table("ducklake_table_stats", meta,
        Column("table_id", BigInteger),
        Column("record_count", BigInteger),
        Column("next_row_id", BigInteger),
        Column("file_size_bytes", BigInteger),
    )

    Table("ducklake_table_column_stats", meta,
        Column("table_id", BigInteger),
        Column("column_id", BigInteger),
        Column("contains_null", Boolean),
        Column("contains_nan", Boolean),
        Column("min_value", Text),
        Column("max_value", Text),
        Column("extra_stats", Text),
    )

    # ── Partitioning ─────────────────────────────────────────────────

    Table("ducklake_partition_info", meta,
        Column("partition_id", BigInteger),
        Column("table_id", BigInteger),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
    )

    Table("ducklake_partition_column", meta,
        Column("partition_id", BigInteger),
        Column("table_id", BigInteger),
        Column("partition_key_index", BigInteger),
        Column("column_id", BigInteger),
        Column("transform", Text),
    )

    Table("ducklake_file_partition_value", meta,
        Column("data_file_id", BigInteger),
        Column("table_id", BigInteger),
        Column("partition_key_index", BigInteger),
        Column("partition_value", Text),
    )

    # ── Column mapping ───────────────────────────────────────────────

    Table("ducklake_column_mapping", meta,
        Column("mapping_id", BigInteger),
        Column("table_id", BigInteger),
        Column("type", Text),
    )

    Table("ducklake_name_mapping", meta,
        Column("mapping_id", BigInteger),
        Column("column_id", BigInteger),
        Column("source_name", Text),
        Column("target_field_id", BigInteger),
        Column("parent_column", BigInteger),
        Column("is_partition", Boolean),
    )

    # ── Deletion scheduling / inlined data ───────────────────────────

    Table("ducklake_files_scheduled_for_deletion", meta,
        Column("data_file_id", BigInteger),
        Column("path", Text),
        Column("path_is_relative", Boolean),
        Column("schedule_start", DateTime(timezone=True)),
    )

    Table("ducklake_inlined_data_tables", meta,
        Column("table_id", BigInteger),
        Column("table_name", Text),
        Column("schema_version", BigInteger),
    )

    # ── Macros ───────────────────────────────────────────────────────

    Table("ducklake_macro", meta,
        Column("schema_id", BigInteger),
        Column("macro_id", BigInteger),
        Column("macro_name", Text),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
    )

    Table("ducklake_macro_impl", meta,
        Column("macro_id", BigInteger),
        Column("impl_id", BigInteger),
        Column("dialect", Text),
        Column("sql", Text),
        Column("type", Text),
    )

    Table("ducklake_macro_parameters", meta,
        Column("macro_id", BigInteger),
        Column("impl_id", BigInteger),
        Column("column_id", BigInteger),
        Column("parameter_name", Text),
        Column("parameter_type", Text),
        Column("default_value", Text),
        Column("default_value_type", Text),
    )

    # ── Sort info ────────────────────────────────────────────────────

    Table("ducklake_sort_info", meta,
        Column("sort_id", BigInteger),
        Column("table_id", BigInteger),
        Column("begin_snapshot", BigInteger),
        Column("end_snapshot", BigInteger),
    )

    Table("ducklake_sort_expression", meta,
        Column("sort_id", BigInteger),
        Column("table_id", BigInteger),
        Column("sort_key_index", BigInteger),
        Column("expression", Text),
        Column("dialect", Text),
        Column("sort_direction", Text),
        Column("null_order", Text),
    )

    return meta


def create_catalog(engine, schema=None):
    """Create all DuckLake catalog tables in the given database.
    If schema is provided, creates the schema first (PG/DuckDB)."""
    meta = _build_metadata(schema=schema)

    if schema:
        from sqlalchemy import text
        with engine.connect() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            conn.commit()

    meta.create_all(engine)
    return meta


# Default metadata instance (no schema qualifier)
DUCKLAKE_METADATA = _build_metadata()
