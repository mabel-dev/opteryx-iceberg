"""Avro and ORC data files on gs:// and s3://, read through SQL
(opteryx-core docs/REMOTE_AVRO_ORC_DATA_FILES_DESIGN.md, phase 1).

No live bucket is ever touched. A local http.server plays object storage, and the filesystem
factory (`opteryx.connectors.io_systems.create_filesystem`) is the seam: a fake filesystem
declares, as the real GCS and S3 ones do, ONE way a native read authenticates - a bearer
header (GCS) or a signed URL (S3). The store refuses an unauthenticated request, so a test
that reads rows proves the credential reached the native Source.
"""

import http.server
import threading
import urllib.parse

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

from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import NotSupportedError

BEARER = "Bearer test-token"
EXPECTED_IDS = [1, 2, 3, 4, 5]


class Store(http.server.ThreadingHTTPServer):
    """object name -> bytes, a record of every request, and an authentication rule."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), _Handler)
        self.objects = {}
        self.requests = []
        self.rule = lambda path, query, headers: 200
        threading.Thread(target=self.serve_forever, daemon=True).start()

    @property
    def base(self):
        return f"http://127.0.0.1:{self.server_address[1]}"


class _Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):  # pragma: no cover
        pass

    def do_GET(self):
        parsed = urllib.parse.urlsplit(self.path)
        path = urllib.parse.unquote(parsed.path).lstrip("/")
        self.server.requests.append((path, parsed.query, dict(self.headers)))
        status = self.server.rule(path, parsed.query, self.headers)
        if status == 200 and path not in self.server.objects:
            status = 404
        body = self.server.objects[path] if status == 200 else b"refused"
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class BearerFilesystem:
    """GCS-like: a bearer header, unsigned URLs."""

    signs_urls = False

    def __init__(self, base):
        self.base = base

    def native_auth_header(self):
        return BEARER

    def object_url(self, path):
        return f"{self.base}/{path.split('://', 1)[1]}"

    def rewrite_to_signed_url(self, path, expiry_seconds=3600):  # pragma: no cover
        raise AssertionError("a bearer filesystem must not sign")


class SigningFilesystem:
    """S3-like: the URL is the credential, no header."""

    signs_urls = True

    def __init__(self, base, signature="ok"):
        self.base = base
        self.signature = signature

    def native_auth_header(self):
        return None

    def rewrite_to_signed_url(self, path, expiry_seconds=3600):
        return f"{self.base}/{path.split('://', 1)[1]}?X-Sig={self.signature}"

    def object_url(self, path):  # pragma: no cover
        raise AssertionError("a signing filesystem must not be asked for an unsigned URL")


@pytest.fixture
def store():
    server = Store()
    yield server
    server.shutdown()


def _avro_object(root):
    path = root / "src.avro"
    write_avro_data_file(path, FIELDS, ROWS)
    return path.read_bytes(), FileFormat.AVRO, ".avro"


def _orc_object(root):
    return orc_bytes(orc_table(range(1, 6)), IDS), FileFormat.ORC, ".orc"


@pytest.fixture(params=["avro", "orc"])
def remote(request, tmp_path, store, monkeypatch):
    """(env, store, set_filesystem): a table whose one data file is gs://bucket/t/a.<ext>,
    served by `store`."""
    env = AvroEnv(tmp_path)
    data, file_format, ext = (_avro_object if request.param == "avro" else _orc_object)(tmp_path)
    store.objects[f"bucket/t/a{ext}"] = data
    env.add(data_file(f"gs://bucket/t/a{ext}", file_format, 5, len(data)))

    def install(filesystem):
        import opteryx.connectors.io_systems as io_systems

        monkeypatch.setattr(io_systems, "create_filesystem", lambda protocol: filesystem)

    env.install = install
    env.format = request.param
    return env


def test_a_bearer_header_reaches_the_native_source(remote, store):
    store.rule = lambda path, query, headers: 200 if headers.get("Authorization") == BEARER else 401
    remote.install(BearerFilesystem(store.base))
    assert [r[0] for r in remote.rows(f"SELECT id FROM {remote.name}")] == EXPECTED_IDS
    assert remote.rows(f"SELECT COUNT(*) FROM {remote.name} WHERE id > 2") == [(3,)]
    assert store.requests, "the file was never fetched"
    for _, query, headers in store.requests:
        assert headers["Authorization"] == BEARER
        assert headers["Accept-Encoding"] == "identity"
        assert query == ""


def test_a_signed_url_is_fetched_with_no_authorization_header(remote, store):
    store.rule = lambda path, query, headers: 200 if query == "X-Sig=ok" and "Authorization" not in headers else 403
    remote.install(SigningFilesystem(store.base))
    assert [r[0] for r in remote.rows(f"SELECT id FROM {remote.name}")] == EXPECTED_IDS
    for _, query, headers in store.requests:
        assert query == "X-Sig=ok"
        assert "Authorization" not in headers
        assert headers["Accept-Encoding"] == "identity"


def test_a_rejected_credential_fails_the_query_and_leaks_nothing(remote, store):
    store.rule = lambda path, query, headers: 200 if query == "X-Sig=ok" else 403
    remote.install(SigningFilesystem(store.base, signature="expired-secret"))
    with pytest.raises(DatasetReadError) as caught:
        remote.rows(f"SELECT id FROM {remote.name}")
    assert "expired-secret" not in str(caught.value)
    store.rule = lambda path, query, headers: 200 if headers.get("Authorization") == BEARER else 401
    remote.install(BearerFilesystem(store.base))
    store.requests.clear()
    remote.rows(f"SELECT id FROM {remote.name}")   # and the right credential still works afterwards
    assert store.requests


def test_a_missing_object_fails_the_query(remote, store):
    store.objects.clear()
    remote.install(BearerFilesystem(store.base))
    with pytest.raises(DatasetReadError):
        remote.rows(f"SELECT id FROM {remote.name}")


def test_a_filesystem_declaring_no_credential_is_never_asked_to_send_an_anonymous_read(remote, store):
    class Neither:
        signs_urls = False

        def native_auth_header(self):
            return None

    remote.install(Neither())
    with pytest.raises(NotSupportedError, match="declares neither signed URLs nor an authorization header"):
        remote.rows(f"SELECT id FROM {remote.name}")
    assert store.requests == []


@pytest.mark.parametrize("uri", ["s3a://bucket/t/a", "gcs://bucket/t/a"])
@pytest.mark.parametrize("fmt", ["avro", "orc"])
def test_a_scheme_the_sources_do_not_read_is_refused_naming_it(tmp_path, uri, fmt):
    env = AvroEnv(tmp_path)
    ext, file_format = (".avro", FileFormat.AVRO) if fmt == "avro" else (".orc", FileFormat.ORC)
    env.add(data_file(uri + ext, file_format, 5, 100))
    with pytest.raises(NotSupportedError, match=r"scheme '(s3a|gcs)://'"):
        env.rows(f"SELECT id FROM {env.name}")
