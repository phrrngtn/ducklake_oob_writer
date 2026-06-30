"""Maintenance for DuckLake catalogs — delegated to DuckLake's own engine.

The OOB writer deliberately hand-writes catalog rows so it can stamp snapshots
with the *source's* transaction-time. Maintenance is the opposite case:

  * it is genuinely complex (rewriting data files, rewriting delete vectors,
    garbage-collecting unreferenced files), and
  * it is a *system* operation whose natural timestamp really is `now()`.

So we do **not** reimplement it in SQLAlchemy. We attach the catalog with
DuckDB's `ducklake` extension and CALL the native maintenance functions:

  * ``ducklake_merge_adjacent_files``  — compact many small Parquet files
  * ``ducklake_expire_snapshots``      — drop old snapshots (ends time-travel to them)
  * ``ducklake_cleanup_old_files``     — delete files no longer referenced by any snapshot

Boundary
--------
This package **never reads, merges, or rewrites Parquet data files for
compaction**. The actual data-file work is done entirely by DuckDB's native
``ducklake`` engine; the functions here only ``ATTACH`` the catalog and issue a
single ``CALL ducklake_*(...)``. The package's *only* contribution to compaction is to
*enable* it: the OOB writer emits, at registration time, the one thing the native
compaction planner actually requires — a per-table ``ducklake_schema_versions``
row with a non-NULL ``table_id`` (written by ``create_table``) — plus the row-id
bookkeeping (``ducklake_table_stats`` / ``row_id_start``). Per-column
``ducklake_file_column_stats`` are for query pruning, NOT compaction. (Reading a
Parquet file to *compute* pruning stats in ``DuckLakeWriter.register_parquet`` is
metadata work, not compaction.) This boundary is enforced by
``tests/test_native_compaction.py``.

This module needs the ``duckdb`` package, which is an optional dependency::

    uv add "ducklake-oob-writer[maintenance]"

`duckdb` is imported lazily so the core package keeps its single (SQLAlchemy)
runtime dependency.
"""
from __future__ import annotations

from contextlib import contextmanager

__all__ = [
    "attach_lake",
    "lake_reader",
    "compact",
    "expire_snapshots",
    "cleanup_old_files",
    "run_maintenance",
]

_ALIAS = "lake"


def _q(s) -> str:
    """Escape a value for inclusion in a single-quoted DuckDB SQL string."""
    return str(s).replace("'", "''")


def _backend_extension(catalog: str):
    """Infer the storage extension a DuckLake catalog connection string needs."""
    head = catalog.split(":", 1)[0].lower()
    if head == "sqlite":
        return "sqlite"
    if head in ("postgres", "postgresql"):
        return "postgres"
    return None  # a bare DuckDB catalog file needs no extra extension


@contextmanager
def attach_lake(catalog: str, data_path: str, *, alias: str = _ALIAS,
                con=None, read_only: bool = False):
    """Attach a DuckLake catalog via DuckDB's ``ducklake`` extension.

    Args:
        catalog: the DuckLake catalog connection string *without* the ``ducklake:``
            prefix — e.g. ``"sqlite:/lake/catalog.sqlite"``,
            ``"postgres:dbname=lake host=dc1"``, or a DuckDB file path.
        data_path: the data directory passed at ``init_catalog`` time.
        alias: schema alias for the attached lake (default ``"lake"``).
        con: an existing ``duckdb`` connection to reuse; a fresh in-memory one is
            created if omitted.
        read_only: attach the lake read-only.

    Yields the duckdb connection with the lake attached as ``alias``.
    """
    import duckdb  # lazy: keeps duckdb an optional dependency

    own = con is None
    con = con or duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    ext = _backend_extension(catalog)
    if ext:
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    dp = str(data_path).rstrip("/") + "/"
    ro = ", READ_ONLY" if read_only else ""
    con.execute(f"ATTACH 'ducklake:{_q(catalog)}' AS {alias} (DATA_PATH '{_q(dp)}'{ro})")
    try:
        yield con
    finally:
        if own:
            con.close()


@contextmanager
def lake_reader(catalog: str, data_path: str, *, alias: str = _ALIAS, read_only: bool = True):
    """A **SQLAlchemy** connection (duckdb-engine) with the DuckLake catalog attached as
    ``alias``. Read ``lake.*`` tables with SA Core ``select()``; for duckdb-specific SQL
    (Parquet-scan table functions, ``AT (TIMESTAMP …)`` time-travel) use
    ``sqlalchemy.text()`` on the *same* connection. This is the read counterpart of the
    SA-Core catalog writer — one engine abstraction for DuckDB, native ``duckdb`` reserved
    for driver-level needs.

    Needs ``duckdb-engine`` (the ``duckdb`` SQLAlchemy dialect). Yields a SA ``Connection``.
    """
    from sqlalchemy import create_engine, text

    eng = create_engine("duckdb:///:memory:")
    con = eng.connect()
    con.execute(text("INSTALL ducklake"))
    con.execute(text("LOAD ducklake"))
    ext = _backend_extension(catalog)
    if ext:
        con.execute(text(f"INSTALL {ext}"))
        con.execute(text(f"LOAD {ext}"))
    dp = str(data_path).rstrip("/") + "/"
    ro = ", READ_ONLY" if read_only else ""
    con.execute(text(f"ATTACH 'ducklake:{_q(catalog)}' AS {alias} (DATA_PATH '{_q(dp)}'{ro})"))
    try:
        yield con
    finally:
        con.close()
        eng.dispose()


def compact(catalog: str, data_path: str, *, con=None) -> None:
    """Compact adjacent small Parquet files into larger ones.

    Works for any file registered through ``DuckLakeWriter.create_table`` +
    ``register_data_file``. The compaction planner
    (``DuckLakeMetadataManager::GetFilesForCompaction``) reads each table's
    per-table ``ducklake_schema_versions`` row by ``table_id`` and consumes the
    resulting ``schema_version`` with an *unchecked* read — so the only hard
    requirement is that per-table row, which ``create_table`` always emits.
    ``column_stats`` are **not** needed for compaction (they add query-pruning
    stats only — see the design doc).

    The only way this raises a DuckDB ``InternalException``
    ("GetValueInternal on a value that is NULL") is a catalog missing that
    per-table ``schema_versions`` row — e.g. one written by a pre-fix version of
    this package, or by another tool. :func:`expire_snapshots` and
    :func:`cleanup_old_files` never depend on it.
    """
    with attach_lake(catalog, data_path, con=con) as c:
        c.execute(f"CALL ducklake_merge_adjacent_files('{_ALIAS}')")


def expire_snapshots(catalog: str, data_path: str, *, older_than, con=None) -> None:
    """Expire snapshots older than ``older_than`` (a datetime/ISO string).

    Expired snapshots can no longer be time-travelled to; their now-unreferenced
    files become eligible for :func:`cleanup_old_files`. ``older_than`` is
    required — there is intentionally no default, because a careless default
    (e.g. ``now()``) would discard *all* history.
    """
    if older_than is None:
        raise ValueError("expire_snapshots requires older_than (no default — it would expire all history)")
    with attach_lake(catalog, data_path, con=con) as c:
        c.execute(
            f"CALL ducklake_expire_snapshots('{_ALIAS}', older_than => TIMESTAMP '{_q(older_than)}')"
        )


def cleanup_old_files(catalog: str, data_path: str, *, cleanup_all: bool = True,
                      dry_run: bool = False, con=None):
    """Delete data files no longer referenced by any live snapshot.

    Returns the list of (path, ...) rows the call reports. With ``dry_run=True``
    nothing is deleted; the candidate files are returned for inspection.
    """
    flags = f"cleanup_all => {str(cleanup_all).lower()}, dry_run => {str(dry_run).lower()}"
    with attach_lake(catalog, data_path, con=con) as c:
        return c.execute(f"CALL ducklake_cleanup_old_files('{_ALIAS}', {flags})").fetchall()


def run_maintenance(catalog: str, data_path: str, *, older_than=None,
                    attempt_compaction: bool = True) -> dict:
    """Run a standard maintenance pass: (optionally) compact, expire, cleanup.

    Each step runs on its own connection so one failing step cannot corrupt the
    others. ``expire_snapshots`` runs only if ``older_than`` is given
    (history-preserving by default).

    **Compaction note:** files registered through ``create_table`` +
    ``register_data_file`` are compactable (see :func:`compact`). The only catalogs
    that fail are those missing a per-table ``schema_versions`` row (e.g. pre-fix or
    foreign catalogs); ``run_maintenance`` catches that and records it in
    ``summary["compact_error"]`` rather than failing the whole pass. Set
    ``attempt_compaction=False`` to skip it.

    Returns a summary dict: ``compacted``, ``compact_error``, ``expired``,
    ``cleaned_files``.
    """
    summary = {"compacted": False, "compact_error": None, "expired": False, "cleaned_files": 0}
    if attempt_compaction:
        try:
            compact(catalog, data_path)
            summary["compacted"] = True
        except Exception as e:  # incl. duckdb InternalException from the stats gap
            summary["compact_error"] = str(e).splitlines()[0]
    if older_than is not None:
        expire_snapshots(catalog, data_path, older_than=older_than)
        summary["expired"] = True
    cleaned = cleanup_old_files(catalog, data_path)
    summary["cleaned_files"] = len(cleaned or [])
    return summary
