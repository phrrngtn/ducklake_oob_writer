"""S3 / object-store support: write Parquet to s3://, footer_and_size over s3,
register_parquet with a configured con + storage_options, native ducklake read back.

Guarded: runs only when MINIO_USER/MINIO_PW are set (plus optional MINIO_HOST,
LAKE_BUCKET). Point it at any S3-compatible store.

    MINIO_USER=... MINIO_PW=... MINIO_HOST=dc1:9000 LAKE_BUCKET=ha-lake \
        uv run --group dev pytest tests/test_s3.py -v
"""
import datetime as dt
import os
import tempfile
import uuid

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("s3fs")

import duckdb
from sqlalchemy import create_engine

import ducklake_oob_writer as dl

MINIO_USER = os.environ.get("MINIO_USER")
MINIO_PW = os.environ.get("MINIO_PW")

pytestmark = pytest.mark.skipif(
    not (MINIO_USER and MINIO_PW),
    reason="set MINIO_USER/MINIO_PW (+ optional MINIO_HOST, LAKE_BUCKET) to run the S3 test",
)


def test_s3_write_register_read():
    host = os.environ.get("MINIO_HOST", "dc1:9000")
    bucket = os.environ.get("LAKE_BUCKET", "ha-lake")
    run = uuid.uuid4().hex[:8]
    data = f"s3://{bucket}/pytest-{run}/data"        # unique per run
    cat = f"sqlite:{tempfile.mktemp(suffix='.sqlite')}"
    so = {"key": MINIO_USER, "secret": MINIO_PW,
          "client_kwargs": {"endpoint_url": f"http://{host}"}}

    def s3con():
        c = duckdb.connect()
        c.execute("INSTALL httpfs; LOAD httpfs; INSTALL sqlite; LOAD sqlite;")
        c.execute(f"CREATE SECRET m (TYPE s3, KEY_ID '{MINIO_USER}', SECRET '{MINIO_PW}', "
                  f"ENDPOINT '{host}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')")
        return c

    eng = create_engine(cat.replace("sqlite:", "sqlite:///"))
    dl.create_catalog(eng)
    w = dl.DuckLakeWriter(eng, dl.DUCKLAKE_METADATA)
    w.init_catalog(data_path=data)
    w.create_table("main", "t", columns=[("i", "int64")])

    con = s3con()
    fp = f"{data}/main/t/b-{run}.parquet"
    con.execute(f"COPY (SELECT * FROM range(5) AS t(i)) TO '{fp}' (FORMAT PARQUET)")

    fsize, footer = dl.footer_and_size(fp, storage_options=so)   # fsspec over s3
    assert fsize > 0 and footer > 0

    w.register_parquet("t", fp, con=con, storage_options=so, snapshot_time=dt.datetime(2026, 4, 1))
    eng.dispose()

    r = s3con()
    r.execute(f"ATTACH 'ducklake:{cat}' AS lake (DATA_PATH '{data}/')")
    assert r.execute("SELECT count(*) FROM lake.t").fetchone()[0] == 5
