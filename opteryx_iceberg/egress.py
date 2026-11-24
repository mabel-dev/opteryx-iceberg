"""Where a TENANT's REST catalog may make Opteryx connect.

A workspace owner chooses their catalog's `uri` and `oauth2-server-uri`, and the
process that connects is inside Opteryx's network: a VM with the metadata server,
its own local ports, and the VPC's other hosts. https-only is not enough on its
own. pyiceberg's REST client is a plain `requests.Session`, which follows a
redirect wherever it points (https to http included), resolves the name again for
each connection (so a name can answer public for a check and private for the
connect), and quotes an unparseable response body in its error - which reaches
the tenant. Together those make the catalog an internal HTTP client with its
responses reflected back.

`harden(session)` closes all three for a session pyiceberg is about to use:

- every connection goes to an address checked to be public, and to exactly the
  address that was checked (`_PinnedHTTPSConnection`): no re-resolution between
  check and connect. TLS still verifies the certificate against the HOSTNAME;
- a redirect is refused, never followed (`max_redirects = 0`);
- plain http is refused outright, and the process's proxy / CA environment is not
  consulted (`trust_env = False`), so no configured proxy re-resolves the name
  on our behalf.

Used for tenant catalogs only (`_TenantRestCatalog` in opteryx_iceberg.metastore):
an operator's catalog may legitimately be private - a local Lakekeeper, a VPC
service - and is not hardened.
"""

from __future__ import annotations

import ipaddress
import socket
import sys

from requests import Session
from requests.adapters import BaseAdapter
from requests.adapters import HTTPAdapter
from requests.exceptions import InvalidSchema
from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.exceptions import ConnectTimeoutError
from urllib3.exceptions import NameResolutionError
from urllib3.exceptions import NewConnectionError
from urllib3.util import connection


class BlockedAddress(PermissionError):
    """A tenant catalog's host resolves to an address Opteryx will not connect to."""


def is_public(address) -> bool:
    """Whether Opteryx may connect to `address` on a tenant's behalf.

    `is_global` rather than a list of private ranges: it also excludes shared
    address space (100.64.0.0/10, which cloud NATs use), benchmarking and
    documentation ranges, and an IPv4-mapped IPv6 address of any of them.
    Multicast is excluded separately because some multicast ranges are global.
    """
    return address.is_global and not address.is_multicast


def public_address(host: str, port: int) -> str:
    """The address to connect to for `host`: the first it resolves to, provided
    EVERY address it resolves to is public. Raises BlockedAddress otherwise.

    All of them, not the first: a name answering one public and one private
    address must be refused, since resolver order decides nothing. The message
    names the host, never the address - the tenant typed the host, and the
    address would be a piece of internal topology.
    """
    infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    addresses = [ipaddress.ip_address(info[4][0]) for info in infos]
    if not addresses or not all(is_public(address) for address in addresses):
        raise BlockedAddress(
            f"catalog host {host!r} is not at a public address; Opteryx connects to "
            "public https catalogs only"
        )
    return str(addresses[0])


class _PinnedHTTPSConnection(HTTPSConnection):
    """urllib3's HTTPSConnection, connecting to the address `public_address`
    vetted rather than resolving the name again.

    Only the socket's destination changes. `self.host` - what TLS sends as SNI
    and verifies the certificate against - is still the hostname. The exception
    mapping is urllib3's own `_new_conn`, so requests reports a failure as it
    would any other connection error.
    """

    def _new_conn(self) -> socket.socket:
        address = public_address(self._dns_host, self.port)
        try:
            sock = connection.create_connection(
                (address, self.port),
                self.timeout,
                source_address=self.source_address,
                socket_options=self.socket_options,
            )
        except socket.gaierror as e:
            raise NameResolutionError(self.host, self, e) from e
        except TimeoutError as e:
            raise ConnectTimeoutError(
                self,
                f"Connection to {self.host} timed out. (connect timeout={self.timeout})",
            ) from e
        except OSError as e:
            raise NewConnectionError(self, f"Failed to establish a new connection: {e}") from e
        sys.audit("http.client.connect", self, self.host, self.port)
        return sock


class _PinnedHTTPSConnectionPool(HTTPSConnectionPool):
    ConnectionCls = _PinnedHTTPSConnection


class _PinnedHTTPSAdapter(HTTPAdapter):
    """requests' HTTPAdapter whose https pools open `_PinnedHTTPSConnection`s."""

    def init_poolmanager(self, *args, **kwargs) -> None:
        super().init_poolmanager(*args, **kwargs)
        # https only: an http pool cannot be built from this adapter at all.
        self.poolmanager.pool_classes_by_scheme = {"https": _PinnedHTTPSConnectionPool}


class _RefusePlainHttp(BaseAdapter):
    def send(self, request, **kwargs):
        raise InvalidSchema(
            "a tenant's Iceberg catalog is reached over https only; refusing "
            f"{request.method} to a plain http address"
        )

    def close(self) -> None:
        return None


def harden(session: Session) -> Session:
    """Make `session` connect only to public https addresses, pinned, with no
    redirects and no environment proxies. Returns the same session."""
    session.trust_env = False
    session.max_redirects = 0
    session.mount("https://", _PinnedHTTPSAdapter())
    session.mount("http://", _RefusePlainHttp())
    return session
