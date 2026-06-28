"""Drift guard for the static catalog interface.

The minimal-interface / subset-INSERT approach breaks in exactly one way: DuckLake
adds a *mandatory* column — non-nullable **and** without a default — to a table we
declare, that we don't populate. Then our INSERT fails the NOT-NULL constraint.

This catches precisely that, and nothing more, in one set-based pass: let the
`ducklake` extension create a real (version-correct) catalog, ask it for the
mandatory columns of every `ducklake_*` table, and assert that for every table we
*declare* we already have a record of each such column. Tables we don't touch are
ignored. Reader-level correctness (a write reads back right) is covered separately
by the round-trip operation tests — this is only the mechanical "will the INSERT
still succeed" contract.

Run: uv run --group dev pytest tests/test_schema_drift.py -v
"""
import os

import pytest

pytest.importorskip("duckdb")
import duckdb

import ducklake_oob_writer as dl


def test_no_uncovered_mandatory_columns(tmp_path):
    # the tables (and the columns) WE declare / interact with
    declared = {name.split(".")[-1]: {c.name for c in t.columns}
                for name, t in dl.DUCKLAKE_METADATA.tables.items()}

    # let the ducklake extension build a real, version-correct catalog
    cat = os.path.join(str(tmp_path), "cat.ducklake")
    data = os.path.join(str(tmp_path), "data")
    os.makedirs(data, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL ducklake; LOAD ducklake;")
    con.execute(f"ATTACH 'ducklake:{cat}' AS lake (DATA_PATH '{data}/')")
    con.execute("DETACH lake")
    con.execute(f"ATTACH '{cat}' AS meta (READ_ONLY)")
    # one set-based query: mandatory (NOT NULL, no default) columns of every ducklake_* table
    mandatory = con.execute(
        "SELECT table_name, column_name FROM duckdb_columns() "
        "WHERE database_name = 'meta' AND table_name LIKE 'ducklake_%' "
        "AND is_nullable = false AND column_default IS NULL"
    ).fetchall()
    con.close()

    uncovered = [f"{t}.{c}" for t, c in mandatory
                 if t in declared and c not in declared[t]]
    assert not uncovered, (
        "DuckLake now has mandatory (NOT NULL, no-default) columns our catalog "
        f"interface doesn't declare — subset-INSERT would fail on: {uncovered}. "
        "Add them to the declared interface.")
