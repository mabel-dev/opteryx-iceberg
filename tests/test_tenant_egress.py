"""A tenant's REST catalog cannot make Opteryx an HTTP client inside its own network.

The catalog's `uri` and `oauth2-server-uri` are the tenant's, and pyiceberg's REST
client would follow a redirect anywhere (https to http included), re-resolve the
name for each connection, and quote the response body in its error. These tests
pin that a TENANT catalog's sessions (opteryx_iceberg.egress) connect only to a
public address - the one that was checked - never follow a redirect, never speak
plain http, and still verify TLS against the hostname. An operator's catalog is
not hardened (test_catalog_properties reaches a 127.0.0.1 server through it).

The TLS tests run a real local server. 127.0.0.1 is not public, so they treat
exactly that address as public and resolve the test names to it; everything else
is the code under test.
"""

import datetime
import http.server
import ipaddress
import os
import socket
import ssl
import sys
import threading
from unittest.mock import patch

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))

import pytest
import requests

from opteryx_iceberg import egress
from opteryx_iceberg.egress import BlockedAddress
from opteryx_iceberg.egress import harden
from opteryx_iceberg.egress import is_public
from opteryx_iceberg.egress import public_address
from opteryx_iceberg.metastore import _GuardedRestCatalog
from opteryx_iceberg.metastore import _TenantRestCatalog

LOOPBACK = ipaddress.ip_address("127.0.0.1")

# ── which addresses are public ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",  # the worker's own ports
        "10.142.0.7",  # the VPC
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",  # the metadata server
        "100.64.0.1",  # shared address space, used by cloud NAT
        "0.0.0.0",
        "224.0.0.1",
        "198.18.0.1",  # benchmarking
        "::1",
        "fe80::1",
        "fd00::1",
        "::ffff:10.142.0.7",  # IPv4-mapped private
        "::ffff:169.254.169.254",
    ],
)
def test_internal_addresses_are_not_public(address):
    assert not is_public(ipaddress.ip_address(address))


@pytest.mark.parametrize("address", ["8.8.8.8", "34.117.0.1", "2001:4860:4860::8888"])
def test_internet_addresses_are_public(address):
    assert is_public(ipaddress.ip_address(address))


def _resolving(answers):
    """socket.getaddrinfo answering `answers[host]` (a list of addresses) for the
    named hosts, and passing IP literals through unchanged."""
    real = socket.getaddrinfo

    def fake(host, port, *args, **kwargs):
        if host in answers:
            return [
                (
                    socket.AF_INET6 if ":" in a else socket.AF_INET,
                    socket.SOCK_STREAM,
                    6,
                    "",
                    (a, port),
                )
                for a in answers[host]
            ]
        return real(host, port, *args, **kwargs)

    return patch("socket.getaddrinfo", side_effect=fake)


def test_a_host_with_any_private_address_is_refused():
    """Resolver order decides nothing: one private answer among public ones refuses."""
    with (
        _resolving({"mixed.example": ["8.8.8.8", "10.142.0.7"]}),
        pytest.raises(BlockedAddress) as caught,
    ):
        public_address("mixed.example", 443)
    # the tenant typed the host; the address is internal topology and is not echoed
    assert "mixed.example" in str(caught.value)
    assert "10.142" not in str(caught.value)


def test_a_public_host_resolves_to_its_first_address():
    with _resolving({"catalog.example": ["8.8.8.8", "8.8.4.4"]}):
        assert public_address("catalog.example", 443) == "8.8.8.8"


# ── a hardened session, without a network ─────────────────────────────────────


@pytest.fixture
def no_socket(monkeypatch):
    """Fail the test if a connection is opened: a refusal must come first."""

    def refuse(*args, **kwargs):
        raise AssertionError("a refused address was connected to")

    monkeypatch.setattr(egress.connection, "create_connection", refuse)


@pytest.mark.parametrize(
    "url",
    [
        "https://169.254.169.254/computeMetadata/v1/",
        "https://127.0.0.1:8080/admin",
        "https://10.142.0.7/",
        "https://[::1]/",
        "https://internal.example/v1/config",
    ],
)
def test_a_private_address_is_refused_before_connecting(no_socket, url):
    with (
        _resolving({"internal.example": ["10.142.0.7"]}),
        pytest.raises(requests.ConnectionError, match="not at a public address"),
    ):
        harden(requests.Session()).get(url)


def test_plain_http_is_refused_before_connecting(no_socket):
    with pytest.raises(requests.exceptions.InvalidSchema, match="https only"):
        harden(requests.Session()).get("http://8.8.8.8/")


def test_the_process_proxy_and_ca_environment_is_not_consulted(monkeypatch):
    """A proxy resolves the name itself, which would undo the pin."""
    monkeypatch.setenv("HTTPS_PROXY", "http://10.142.0.9:3128")
    session = harden(requests.Session())
    assert session.trust_env is False
    assert session.max_redirects == 0


# ── a real TLS server on 127.0.0.1 ────────────────────────────────────────────


def _certificates(tmp_path, hostname):
    """A throwaway CA and a server certificate for `hostname`, as file paths."""
    x509 = pytest.importorskip("cryptography.x509")
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    now = datetime.datetime.now(datetime.UTC)

    def build(subject, issuer, public_key, signing_key, extensions):
        builder = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, subject)]))
            .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, issuer)]))
            .public_key(public_key)
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=5))
            .not_valid_after(now + datetime.timedelta(hours=1))
        )
        for extension, critical in extensions:
            builder = builder.add_extension(extension, critical=critical)
        return builder.sign(signing_key, hashes.SHA256())

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = build(
        "test-ca",
        "test-ca",
        ca_key.public_key(),
        ca_key,
        [
            (x509.BasicConstraints(ca=True, path_length=None), True),
            (
                x509.KeyUsage(
                    digital_signature=True,
                    key_cert_sign=True,
                    crl_sign=True,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                True,
            ),
            (x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False),
        ],
    )
    key = ec.generate_private_key(ec.SECP256R1())
    cert = build(
        hostname,
        "test-ca",
        key.public_key(),
        ca_key,
        [
            (x509.SubjectAlternativeName([x509.DNSName(hostname)]), False),
            (x509.BasicConstraints(ca=False, path_length=None), True),
            (x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False),
        ],
    )
    pem = serialization.Encoding.PEM
    (tmp_path / "ca.pem").write_bytes(ca.public_bytes(pem))
    (tmp_path / "cert.pem").write_bytes(cert.public_bytes(pem))
    (tmp_path / "key.pem").write_bytes(
        key.private_bytes(pem, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
    )
    return str(tmp_path / "ca.pem"), str(tmp_path / "cert.pem"), str(tmp_path / "key.pem")


class _Server:
    """An https server on 127.0.0.1 holding a certificate for catalog.test.
    `routes` maps a path to (status, headers, body); every request is recorded."""

    def __init__(self, tmp_path, routes):
        self.ca, cert, key = _certificates(tmp_path, "catalog.test")
        self.requests = []
        recorded = self.requests

        class Handler(http.server.BaseHTTPRequestHandler):
            def _answer(self):
                recorded.append((self.command, self.path))
                status, headers, body = routes.get(self.path.split("?")[0], (404, {}, b"{}"))
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = _answer
            do_POST = _answer

            def log_message(self, *args):
                pass

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        self.httpd.socket = context.wrap_socket(self.httpd.socket, server_side=True)
        self.port = self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


CONFIG = (200, {"Content-Type": "application/json"}, b'{"defaults": {}, "overrides": {}}')
NAMESPACES = (200, {"Content-Type": "application/json"}, b'{"namespaces": [["ns"]]}')
TOKEN = (
    200,
    {"Content-Type": "application/json"},
    b'{"access_token": "t", "token_type": "bearer", "expires_in": 3600}',
)
REDIRECT_TO_METADATA = (
    302,
    {"Location": "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/"},
    b"",
)


@pytest.fixture
def server(tmp_path):
    servers = []

    def start(routes):
        started = _Server(tmp_path, routes)
        servers.append(started)
        return started

    yield start
    for started in servers:
        started.close()


@pytest.fixture
def loopback_is_public(monkeypatch):
    """127.0.0.1 - and only it - counts as public, so the local server is reachable."""
    real = egress.is_public
    monkeypatch.setattr(egress, "is_public", lambda a: a == LOOPBACK or real(a))


def _session(ca):
    session = harden(requests.Session())
    session.verify = ca  # the throwaway CA; trust_env=False ignores REQUESTS_CA_BUNDLE
    return session


def test_a_pinned_connection_still_verifies_tls_against_the_hostname(server, loopback_is_public):
    catalog = server({"/v1/config": CONFIG})
    with _resolving({"catalog.test": ["127.0.0.1"], "other.test": ["127.0.0.1"]}):
        response = _session(catalog.ca).get(f"https://catalog.test:{catalog.port}/v1/config")
        assert response.json() == {"defaults": {}, "overrides": {}}
        # same address, a name the certificate is not for: refused by TLS, so the
        # socket's address being the IP has not turned off hostname verification
        with pytest.raises(requests.exceptions.SSLError):
            _session(catalog.ca).get(f"https://other.test:{catalog.port}/v1/config")


def test_a_redirect_is_refused_not_followed(server, loopback_is_public):
    catalog = server({"/v1/config": REDIRECT_TO_METADATA})
    with (
        _resolving({"catalog.test": ["127.0.0.1"]}),
        pytest.raises(requests.exceptions.TooManyRedirects),
    ):
        _session(catalog.ca).get(f"https://catalog.test:{catalog.port}/v1/config")
    assert catalog.requests == [("GET", "/v1/config")]


def test_the_connection_goes_to_the_address_that_was_checked(server, loopback_is_public):
    """DNS rebinding: the name answers public for the check, then private. Each
    connection resolves ONCE and connects to that answer, so there is no second
    lookup to rebind - and the next connection is checked afresh."""
    catalog = server({"/v1/config": CONFIG})
    answers = iter([["127.0.0.1"], ["10.142.0.7"]])
    lookups = []
    connected = []
    real_getaddrinfo = socket.getaddrinfo
    real_connect = egress.connection.create_connection

    def rebinding(host, port, *args, **kwargs):
        if host == "catalog.test":
            lookups.append(host)
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (a, port)) for a in next(answers)]
        return real_getaddrinfo(host, port, *args, **kwargs)

    def recording(address, *args, **kwargs):
        connected.append(address[0])
        return real_connect(address, *args, **kwargs)

    url = f"https://catalog.test:{catalog.port}/v1/config"
    with (
        patch("socket.getaddrinfo", side_effect=rebinding),
        patch.object(egress.connection, "create_connection", side_effect=recording),
    ):
        assert _session(catalog.ca).get(url).status_code == 200
        assert lookups == ["catalog.test"] and connected == ["127.0.0.1"]
        # a new connection: the name now answers private, and that is refused
        with pytest.raises(requests.ConnectionError, match="not at a public address"):
            _session(catalog.ca).get(url)
    assert connected == ["127.0.0.1"]


# ── the tenant REST catalog uses it from its first request ────────────────────


@pytest.fixture
def trusted_ca(monkeypatch):
    """Every hardened session trusts `ca` - pyiceberg's own `ssl` property is
    outside the tenant allow-list, so this is the test's way in."""
    holder = {}
    real = egress.harden

    def with_ca(session):
        real(session)
        session.verify = holder["ca"]
        return session

    monkeypatch.setattr("opteryx_iceberg.metastore.harden", with_ca)
    return holder


def test_a_tenant_catalog_works_against_a_public_https_catalog(
    server, loopback_is_public, trusted_ca
):
    catalog = server({"/v1/config": CONFIG, "/v1/namespaces": NAMESPACES})
    trusted_ca["ca"] = catalog.ca
    with _resolving({"catalog.test": ["127.0.0.1"]}):
        rest = _TenantRestCatalog(
            "ws", uri=f"https://catalog.test:{catalog.port}", warehouse="w", **{"py-io-impl": "x"}
        )
        assert rest.list_namespaces() == [("ns",)]


def test_a_tenant_catalog_will_not_follow_its_config_redirect(
    server, loopback_is_public, trusted_ca
):
    catalog = server({"/v1/config": REDIRECT_TO_METADATA})
    trusted_ca["ca"] = catalog.ca
    with (
        _resolving({"catalog.test": ["127.0.0.1"]}),
        pytest.raises(requests.exceptions.TooManyRedirects),
    ):
        _TenantRestCatalog("ws", uri=f"https://catalog.test:{catalog.port}", warehouse="w")
    assert catalog.requests == [("GET", "/v1/config?warehouse=w")]


def test_the_oauth_token_request_is_held_to_the_same_rule(no_socket):
    """pyiceberg fetches a `credential`'s token the moment it builds a session -
    before any catalog request - so the hardening must already be in place."""
    with (
        _resolving({"catalog.test": ["8.8.8.8"], "internal.example": ["10.142.0.7"]}),
        pytest.raises(requests.ConnectionError, match="internal.example.*not at a public"),
    ):
        _TenantRestCatalog(
            "ws",
            uri="https://catalog.test",
            warehouse="w",
            credential="id:secret",
            **{"oauth2-server-uri": "https://internal.example/v1/oauth/tokens"},
        )


def test_an_operator_catalog_is_not_hardened(server):
    """A local Lakekeeper or a VPC service is a legitimate operator catalog."""
    catalog = _GuardedRestCatalog.__new__(_GuardedRestCatalog)
    session = requests.Session()
    catalog.properties = {}
    catalog._config_headers(session)
    assert session.max_redirects == requests.models.DEFAULT_REDIRECT_LIMIT
    assert type(session.get_adapter("https://x")) is requests.adapters.HTTPAdapter
