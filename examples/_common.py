"""Shared helper for the examples: an OOB-written DuckLake on a SQLite catalog.

Keeps the example scripts focused on the concept being demonstrated rather than
on Parquet/footer/registration boilerplate. Not part of the public API.
"""
from __future__ import annotations

from pathlib import Path

import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl


class Lake:
    """A tiny OOB-written DuckLake (SQLite catalog) for demonstrations."""

    def __init__(self, workdir):
        self.workdir = Path(workdir)
        self.data_path = self.workdir / "data"
        self.data_path.mkdir(parents=True, exist_ok=True)
        self.catalog_file = self.workdir / "catalog.sqlite"
        self.catalog = f"sqlite:{self.catalog_file}"          # for `ducklake:` ATTACH
        self.engine = create_engine(f"sqlite:///{self.catalog_file}")
        dl.create_catalog(self.engine)
        self.writer = dl.DuckLakeWriter(self.engine, dl.DUCKLAKE_METADATA)
        self.writer.init_catalog(data_path=str(self.data_path))
        self._seq: dict[str, int] = {}
        self._released = False

    def create_table(self, table, columns):
        """columns: list of (name, ducklake_type) e.g. ('mean','float64')."""
        self.writer.create_table("main", table, columns)
        self._seq[table] = 0

    def append(self, table, select_sql, snapshot_time=None) -> int:
        """Write `select_sql` to a new Parquet file and OOB-register it.

        `snapshot_time` is the SOURCE transaction-time to stamp on the snapshot.
        Returns the row count written.
        """
        self._seq[table] += 1
        fname = f"batch-{self._seq[table]:04d}.parquet"
        phys = self.data_path / "main" / table / fname
        phys.parent.mkdir(parents=True, exist_ok=True)
        c = duckdb.connect()
        c.execute(f"COPY ({select_sql}) TO '{phys}' (FORMAT PARQUET)")
        n = c.execute(f"SELECT count(*) FROM read_parquet('{phys}')").fetchone()[0]
        c.close()
        fsize, footer = dl.footer_and_size(str(phys))
        self.writer.register_data_file(
            table, path=fname, record_count=n,
            file_size_bytes=fsize, footer_size=footer, snapshot_time=snapshot_time,
        )
        return n

    def release(self):
        """Release the SQLAlchemy engine so DuckDB can open the catalog cleanly."""
        if not self._released:
            self.engine.dispose()
            self._released = True

    def reader(self):
        """A duckdb connection with the lake attached (read side)."""
        self.release()
        c = duckdb.connect()
        c.execute("INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;")
        c.execute(f"ATTACH 'ducklake:{self.catalog}' AS lake (DATA_PATH '{self.data_path}/')")
        return c

    def data_files(self):
        """Relative paths of Parquet files currently on disk."""
        return sorted(str(p.relative_to(self.data_path)) for p in self.data_path.rglob("*.parquet"))
