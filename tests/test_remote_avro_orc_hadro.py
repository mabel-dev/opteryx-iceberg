"""Avro and ORC data files on s3://, read through SQL against hadro
(opteryx-core docs/REMOTE_AVRO_ORC_DATA_FILES_DESIGN.md, phase 1).

hadro (mabel-dev/hadro) is a read-only S3 server over a local directory that, with keys set,
verifies the SigV4 signature of every request. So these tests exercise the REAL S3 filesystem:
its credential chain, its presigned URL, the native Source's GET - and a request that is
mis-signed fails here as it would against AWS. No fake filesystem, no live service.

The same table is read from local files as the oracle: the only variable is the transport.
"""

import os
import sys

import pytest
from _avro_data_files import FileFormat
from _avro_data_files import data_file
from _avro_data_files import write_avro_data_file
from _orc_data_files import orc_bytes
from test_avro_data_files import FIELDS
from test_avro_data_files import ROWS
from test_avro_data_files import Env as AvroEnv
from test_orc_data_files import IDS
from test_orc_data_files import _table as orc_table

_NEXTCLOUD = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_hadro_src = os.path.join(_NEXTCLOUD, "hadro", "src")
if os.path.isdir(_hadro_src) and _hadro_src not in sys.path:
    sys.path.insert(1, _hadro_src)
hadro = pytest.importorskip("hadro", reason="hadro (S3 test server) is not importable")

from opteryx.connectors.io_systems.s3_filesystem import reset_credential_cache  # noqa: E402
from opteryx.exceptions import DatasetReadError  # noqa: E402

ACCESS_KEY = "hadro-access"
SECRET_KEY = "hadro-secret"
_AWS_VARS = (
    "AWS_S3_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
    "AWS_PROFILE", "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE", "AWS_EC2_METADATA_DISABLED",
)


def _avro_object(root):
    path = root / "src.avro"
    write_avro_data_file(path, FIELDS, ROWS)
    return path.read_bytes(), FileFormat.AVRO, "avro"


def _orc_object(root):
    return orc_bytes(orc_table(range(1, 6)), IDS), FileFormat.ORC, "orc"


@pytest.fixture(params=["avro", "orc"])
def table(request, tmp_path, monkeypatch):
    """An Iceberg table whose one data file is s3://wh/t/a.<ext>, served by a
    signature-checking hadro; yields (env, local_env, server)."""
    data, file_format, ext = (_avro_object if request.param == "avro" else _orc_object)(tmp_path)
    root = tmp_path / "s3"
    (root / "wh" / "t").mkdir(parents=True)
    (root / "wh" / "t" / f"a.{ext}").write_bytes(data)

    for name in _AWS_VARS:
        monkeypatch.delenv(name, raising=False)
    (tmp_path / "remote").mkdir()
    (tmp_path / "local").mkdir()
    remote = AvroEnv(tmp_path / "remote")
    remote.add(data_file(f"s3://wh/t/a.{ext}", file_format, 5, len(data)))
    local = AvroEnv(tmp_path / "local")
    local_path = local.root / "data" / f"a.{ext}"
    local_path.write_bytes(data)
    local.add(data_file(local_path, file_format, 5, len(data)))

    config = hadro.Config(data=str(root), access_key=ACCESS_KEY, secret_key=SECRET_KEY)
    with hadro.Server(config=config, port=0) as server:
        monkeypatch.setenv("AWS_S3_ENDPOINT", server.endpoint)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", ACCESS_KEY)
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
        monkeypatch.setenv("AWS_REGION", "eu-west-2")
        # never let a missing credential fall through to a real metadata endpoint
        monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
        monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "absent-credentials"))
        monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "absent-config"))
        reset_credential_cache()
        try:
            yield remote, local, server
        finally:
            reset_credential_cache()


SQL = [
    "SELECT id, name, n FROM {t}",
    "SELECT COUNT(*), SUM(n) FROM {t}",
    "SELECT id FROM {t} WHERE id > 2",
    "SELECT COUNT(*) FROM {t} WHERE flag IS TRUE",
]


@pytest.mark.parametrize("sql", SQL)
def test_an_s3_table_reads_the_same_rows_as_the_same_files_on_disk(table, sql):
    remote, local, _ = table
    got = remote.rows(sql.format(t=remote.name))
    assert got == local.rows(sql.format(t=local.name))
    assert got
    assert remote.scan_sources == local.scan_sources


def test_a_wrong_secret_is_refused_by_the_signature_check(table, monkeypatch):
    remote, _, _ = table
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "not-the-secret")
    reset_credential_cache()
    with pytest.raises(DatasetReadError) as caught:
        remote.rows(f"SELECT id FROM {remote.name}")
    assert "not-the-secret" not in str(caught.value)
    assert "X-Amz-Signature" not in str(caught.value)


def test_a_missing_object_is_a_read_error(table, tmp_path):
    remote, _, server = table
    for path in (tmp_path / "s3" / "wh" / "t").iterdir():
        path.unlink()
    with pytest.raises(DatasetReadError):
        remote.rows(f"SELECT id FROM {remote.name}")
