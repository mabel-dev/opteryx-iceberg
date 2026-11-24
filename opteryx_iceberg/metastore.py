"""IcebergMetastore: wraps pyiceberg's own catalog loader (REST/SQL/Hive/Glue,
whichever `catalog_type`/`uri` selects) as an opteryx_catalog Metastore.

Tier 1 reads; Tier 2 writes CREATE SCHEMA, CREATE TABLE and INSERT INTO ...
SELECT (opteryx_iceberg.writes). Registered per-workspace via the existing
`opteryx.connectors.register_workspace(prefix, OpteryxConnector,
catalog=IcebergMetastore, workspace=..., **iceberg_properties)` seam - no new
routing mechanism needed.

Duck-type surface: OpteryxConnector calls a wider set of catalog methods than
the formal `Metastore` ABC (see opteryx-core/opteryx/connectors/
opteryx_connector.py). `load_dataset`, `get_relation`, `dataset_exists`,
`list_datasets`, and `load_view` are implemented for real, since they sit on
the plain-SELECT read path. `create_collection` and `create_dataset` are
implemented; every other DDL/write/materialized-view/trigger-shaped call
(DROP, RENAME, CTAS, ...) raises NotImplementedError.
"""

from __future__ import annotations

from collections.abc import Iterable
from urllib.parse import urlsplit

from opteryx_catalog.catalog.metastore import Metastore
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import TagNotFound
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.typedef import EMPTY_DICT

from opteryx_iceberg.dataset import IcebergDataset
from opteryx_iceberg.egress import harden
from opteryx_iceberg.fileio import PY_IO_IMPL
from opteryx_iceberg.fileio import TENANT_PY_IO_IMPL
from opteryx_iceberg.fileio import IcebergFileIO
from opteryx_iceberg.fileio import OpteryxFileIO
from opteryx_iceberg.fileio import TenantFileIO

# The catalog properties IcebergMetastore accepts - everything a workspace's
# catalog binding can carry, decided from what the hosted product offers
# (control.opteryx's catalog-kinds descriptor and the "Connecting an Iceberg
# Catalog" guide): a REST catalog's address and warehouse, a stored bearer
# token or OAuth2 client credential, Polaris' scope and token endpoint, two
# documented headers, and Google auth to BigLake. Anything else is REFUSED,
# never dropped: pyiceberg imports and calls classes named in its config
# (`auth.impl`, `py-*-impl`), sends ambient credentials to whatever `uri` says
# (`auth.type`), and reads local files (`ssl`, `auth.google.credentials_path`).
TENANT_PROPERTIES = frozenset(
    {"uri", "warehouse", "token", "credential", "scope", "oauth2-server-uri", "auth"}
)
# Header names are case-insensitive, so a `header.<Name>` key is compared lowercased.
TENANT_HEADERS = frozenset({"header.x-iceberg-access-delegation", "header.x-goog-user-project"})
# Ambient Google credentials go to this address and nowhere else. A string prefix,
# not a parsed hostname, so no URL-parser differential can move the token.
GOOGLE_AUTH_URI_PREFIX = "https://biglake.googleapis.com/"

# Properties pyiceberg uses to address, authenticate or load code for its REST
# session. A REST server's /v1/config `defaults` and `overrides` are merged into
# the client's properties BEFORE that session is built, so without this check the
# server - an address the binding chose - could set `auth.type=custom` itself.
_TRANSPORT_KEYS = frozenset(
    {
        "uri",
        "auth",
        "ssl",
        "token",
        "credential",
        "scope",
        "audience",
        "resource",
        "oauth2-server-uri",
        "botocore_session",
    }
)
_TRANSPORT_PREFIXES = ("header.", "rest.", "client.", "py-")


def _require_https(name: str, value: str) -> None:
    parts = urlsplit(value)
    if parts.scheme != "https" or not parts.hostname:
        raise ValueError(f"opteryx-iceberg: {name!r} must be an https:// address with a host.")


def check_tenant_properties(catalog_type: str, properties: dict) -> None:
    """Raise ValueError unless `properties` are a catalog binding IcebergMetastore accepts.

    Exposed so a binding WRITER can refuse a config before storing it; the
    metastore runs it again on every construction regardless.
    """
    if catalog_type != "rest":
        raise ValueError(
            f"opteryx-iceberg: catalog type {catalog_type!r} is not supported - only 'rest'."
        )
    refused = sorted(
        key for key in properties if key not in TENANT_PROPERTIES and key.lower() not in TENANT_HEADERS
    )
    if refused:
        raise ValueError(
            f"opteryx-iceberg: catalog properties {refused} are not allowed "
            f"(allowed: {sorted(TENANT_PROPERTIES | TENANT_HEADERS)})."
        )
    if "uri" not in properties:
        raise ValueError("opteryx-iceberg: a REST catalog binding needs a 'uri'.")
    _require_https("uri", properties["uri"])
    if "oauth2-server-uri" in properties:
        _require_https("oauth2-server-uri", properties["oauth2-server-uri"])
    if "auth" in properties:
        auth = properties["auth"]
        if not ({"type"} <= set(auth) <= {"type", "google"}) or auth["type"] != "google":
            raise ValueError(
                'opteryx-iceberg: the only supported auth is {"type": "google", '
                '"google": {"scopes": [...]}}.'
            )
        if "google" in auth and set(auth["google"]) - {"scopes"}:
            raise ValueError("opteryx-iceberg: Google auth accepts only 'scopes'.")
        if not str(properties["uri"]).startswith(GOOGLE_AUTH_URI_PREFIX):
            raise ValueError(
                f"opteryx-iceberg: Google auth is only sent to {GOOGLE_AUTH_URI_PREFIX} - "
                "Opteryx's own identity is not presented to any other catalog."
            )


class _GuardedRestCatalog(RestCatalog):
    """pyiceberg's RestCatalog, minus the server's ability to reconfigure the client."""

    def _fetch_config(self) -> None:
        configured = dict(self.properties)
        super()._fetch_config()
        served = sorted(
            key
            for key in configured.keys() | self.properties.keys()
            if (key in _TRANSPORT_KEYS or key.startswith(_TRANSPORT_PREFIXES))
            and configured.get(key) != self.properties.get(key)
        )
        if served:
            raise ValueError(
                f"opteryx-iceberg: the REST catalog's /v1/config tried to set {served}. A catalog "
                "server may not change how Opteryx addresses, authenticates to, or loads code for it."
            )

    def _load_file_io(self, properties=EMPTY_DICT, location=None):
        # A table's FileIO is built from the catalog's properties under the table's
        # metadata properties and the load-table `config` - all of which the catalog's
        # owner writes - and pyiceberg imports and calls whatever `py-io-impl` that
        # names. It is fixed to the catalog's OWN configured `py-io-impl` (OpteryxFileIO
        # or TenantFileIO, set by IcebergMetastore), which `_fetch_config` already
        # refuses a server changing - so what is served for a table cannot swap it.
        return super()._load_file_io(
            {**properties, "py-io-impl": self.properties["py-io-impl"]}, location
        )


class _TenantRestCatalog(_GuardedRestCatalog):
    """`_GuardedRestCatalog` for a tenant's catalog: every session pyiceberg makes
    reaches only public https addresses, pinned, with no redirects
    (opteryx_iceberg.egress).

    Hooked at `_config_headers` because it is the first thing pyiceberg's
    `_create_session` does with a new session - before the /v1/config request
    (`_fetch_config` builds its own session the same way) and before the legacy
    OAuth2 manager fetches a token from `oauth2-server-uri` on that session.
    """

    def _config_headers(self, session) -> None:
        harden(session)
        super()._config_headers(session)


class IcebergMetastore(Metastore):
    """An Iceberg catalog as an opteryx_catalog Metastore, for configuration a
    TENANT supplies - a workspace's catalog binding. Properties are checked
    against `check_tenant_properties` before pyiceberg sees them.

    `UnrestrictedIcebergMetastore` is the same class without that check, for
    configuration only the deployment's operator can write.
    """

    # The workspace's opteryx catalog entry holds only a LISTING of each
    # Iceberg table (name and columns, no snapshot, no files); the table itself
    # is read from Iceberg. Declared explicitly rather than inherited - see
    # `Metastore.serves_catalog_records`.
    serves_catalog_records: bool = False

    # The FileIO every Iceberg file is read and written through: a tenant's
    # catalog may name only `TenantLocations` (opteryx_iceberg.fileio), because
    # each of those locations is opened with Opteryx's own identity.
    _file_io = TenantFileIO
    _py_io_impl = TENANT_PY_IO_IMPL

    def __init__(
        self,
        workspace: str,
        catalog_type: str = "rest",
        **properties,
    ):
        check_tenant_properties(catalog_type, properties)
        self._open(workspace, catalog_type, properties)

    def _open(self, workspace: str, catalog_type: str, properties: dict) -> None:
        """
        Args:
            workspace: the Opteryx workspace prefix this instance was
                registered under (via `register_workspace` or a workspace
                resolver). Passed straight through as pyiceberg's catalog
                *name*: `load_catalog(workspace, ...)`.

                For REST/Hive/Glue that name is a local label and nothing
                on the wire depends on it. For `catalog_type="sql"` it is
                part of the data: pyiceberg's `SqlCatalog` stores its name
                in the `catalog_name` column of its metadata tables and
                filters every lookup on it, so tables written under a
                different catalog name are invisible here - `load_dataset`
                gets `NoSuchTableError` and raises a bare `DatasetNotFound`,
                indistinguishable from the table never having existed.
                Whoever wrote a SQL catalog must therefore have used the
                same catalog name as the Opteryx workspace prefix it is
                registered under.
            catalog_type: pyiceberg catalog type ("rest", "sql", "hive",
                "glue", ...). Only "rest" passes `check_tenant_properties`.
            properties: forwarded to pyiceberg (e.g. `uri`, `warehouse`,
                `token`, `credential`, `auth={...}`), which IcebergMetastore
                has already checked. Values may be nested - pyiceberg's own
                config shapes, such as
                `auth={"type": "google", "google": {"scopes": [...]}}`, are
                used directly. A REST catalog is built as
                `_GuardedRestCatalog`, so what its server serves cannot
                reconfigure the client or name the table FileIO.

        This class used to accept flat `auth_type`/`google_auth_scopes`
        kwargs and rebuild pyiceberg's nested `auth` dict internally,
        because opteryx-core's connector cache hashed registration kwargs
        and a dict value broke it. That cache is now keyed by workspace
        name (opteryx-core's resolution-first connector layer), nothing
        hashes config any more, and the flattening is retired - the guard
        below turns a stale flat-style config into a clear error instead
        of silently unauthenticated requests.
        """
        for retired in ("auth_type", "google_auth_scopes"):
            if retired in properties:
                raise ValueError(
                    f"opteryx-iceberg: {retired!r} is retired - pass pyiceberg's own "
                    'nested form instead, e.g. auth={"type": "google", "google": '
                    '{"scopes": ["https://www.googleapis.com/auth/cloud-platform"]}}.'
                )
        # Iceberg metadata files (manifest lists, manifests, metadata.json) are
        # ALWAYS read through opteryx-core's storage clients - one GET each, no
        # HEAD, warm connections, and the identity check in
        # opteryx_iceberg.fileio - never through another FileIO. A caller's
        # `py-io-impl` would be overridden for every manifest read (see
        # load_dataset), so it is refused rather than silently ignored.
        if "py-io-impl" in properties:
            raise ValueError(
                "opteryx-iceberg reads all Iceberg metadata through OpteryxFileIO; "
                f"'py-io-impl' is not configurable (got {properties['py-io-impl']!r})."
            )
        self.workspace = workspace
        iceberg_properties = {"py-io-impl": self._py_io_impl, **properties}
        if catalog_type == "rest":
            self._catalog = self._rest_catalog(workspace, iceberg_properties)
        else:
            self._catalog = load_catalog(workspace, type=catalog_type, **iceberg_properties)
        self.io = IcebergFileIO(properties, self._file_io.new_policy())

    @staticmethod
    def _rest_catalog(workspace: str, properties: dict):
        # A tenant's address is reached only as a public https host (egress).
        return _TenantRestCatalog(workspace, **properties)

    def load_dataset(self, identifier: str, load_history: bool = False) -> IcebergDataset:
        table = self._load_table(identifier)
        # NOT `table.io`. pyiceberg builds a table's FileIO from the catalog's
        # properties overlaid with what the catalog SERVES for that table (a
        # REST load-table `config`, storage credentials), and a served
        # `py-io-impl` wins - Lakekeeper's default warehouse serves
        # `pyiceberg.io.fsspec.FsspecFileIO`, which would replace OpteryxFileIO
        # and take manifest reads off opteryx-core. The merged properties are
        # still the right ones to check identity against, so they are kept and
        # only the implementation is fixed.
        return IcebergDataset(identifier, table, self._file_io(table.io.properties))

    def load_view(self, identifier: str):
        # No Iceberg view-spec support in Tier 1. Callers (e.g.
        # OpteryxConnector._try_load_view) catch any Exception broadly and
        # treat it as "not a view", so raising here is correct, not a stub.
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) does not support Iceberg views."
        )

    def property_graph_exists(self, identifier) -> bool:
        # Property graphs are native-catalog objects; an Iceberg catalog holds none.
        return False

    def get_relation(self, identifier: str):
        """Single-round-trip relation resolution used during query binding.

        Returns ("dataset", IcebergDataset), or (None, None) - never raises,
        this is called unguarded on the plain-SELECT binding path.
        """
        try:
            return "dataset", self.load_dataset(identifier)
        except DatasetNotFound:
            return None, None

    def dataset_exists(self, identifier: str) -> bool:
        return self._catalog.table_exists(self._to_iceberg_identifier(identifier))

    def collection_exists(self, namespace: str) -> bool:
        return self._catalog.namespace_exists(self._to_iceberg_namespace(namespace))

    def list_datasets(self, namespace: str) -> Iterable[str]:
        """Table names within `namespace`, BARE - not namespace-qualified.

        This matches the native catalog, which returns the dataset document's
        own id (`OpteryxCatalog.list_datasets` -> `doc.id`), and it is what
        information_schema requires: it pairs each name with the collection it
        asked for (`table_schema=collection, table_name=name`), so a qualified
        name here becomes `ns.ns.t` and every lookup built from it fails with
        DatasetNotFound.

        pyiceberg returns the FULL identifier tuple ("ns", "t") - the namespace
        is the leading part and the table name is the last element, nested
        namespaces included (("a", "b", "t") is table "t" in namespace "a.b").
        """
        return [identifier[-1] for identifier in self._catalog.list_tables(namespace)]

    # ------------------------------------------------------------------
    # information_schema's listing surface.
    #
    # opteryx-core's information_schema walks a catalog with six methods:
    # list_collections -> list_datasets -> list_views / list_triggers, plus
    # load_dataset / load_view. Only the first three are answerable from an
    # Iceberg catalog; without them `SELECT ... FROM <ws>.information_schema.
    # tables` died on AttributeError rather than on anything a user could act
    # on.
    #
    # `list_views` and `list_triggers` return EMPTY rather than raising, and
    # that is a real answer, not a stub: an Iceberg catalog has no triggers at
    # all (they are an opteryx concept), and Tier 1 does not read the Iceberg
    # view spec, so there are none of either to report. Raising would take
    # information_schema down over a table listing that is otherwise correct -
    # the datasets are the part anyone is asking for.
    # ------------------------------------------------------------------

    def list_collections(self) -> Iterable[str]:
        """Namespaces, dotted, matching the shape `list_datasets` takes back.

        pyiceberg hands back tuples ("ns",) / ("a", "b") for nested
        namespaces; opteryx speaks one dotted string.
        """
        return [".".join(namespace) for namespace in self._catalog.list_namespaces()]

    def list_views(self, namespace: str) -> Iterable[str]:
        """Always empty - Tier 1 does not read the Iceberg view spec.

        See the section comment above for why this is empty rather than
        NotImplementedError, unlike `load_view`: that answers "give me THIS
        view", where being wrong matters; this answers "which views are
        there", and for a Tier 1 reader the truthful answer is none.
        """
        return []

    def list_triggers(self, identifier: str) -> Iterable[str]:
        """Always empty - triggers are an opteryx concept with no Iceberg
        equivalent, so there is nothing an Iceberg catalog could report."""
        return []

    # ------------------------------------------------------------------
    # Tags.
    #
    # An Iceberg table keeps its tags in `metadata.refs` - the same map that
    # holds its branches - so both methods below are answered from table
    # metadata already fetched by `load_table`, with no extra round trip
    # beyond that load.
    #
    # BRANCHES ARE DELIBERATELY EXCLUDED. the `tags` column of `SHOW SNAPSHOTS`
    # answers "why is this snapshot still here", and opteryx has no branch
    # concept to show one under; every Iceberg table also carries a `main`
    # branch, which would otherwise appear as a tag on one row of every
    # listing and read as something a user could time travel to by name.
    # ------------------------------------------------------------------

    def list_tags(self, identifier: str) -> list[dict]:
        """Every tag on a table, as the plain dicts opteryx-core expects.

        Same shape as `OpteryxCatalog.list_tags` - `name` and `snapshot-id`,
        ordered by name - because `OpteryxConnector._snapshot_rows` groups on
        `tag["snapshot-id"]` and prints `tag["name"]` without knowing which
        catalog produced them.

        Iceberg carries no per-tag creation time or author, so those keys are
        absent rather than filled with a placeholder: a caller reading them
        off a native-catalog tag would get a real value and here would get an
        invention.
        """
        refs = self._load_table(identifier).metadata.refs
        return sorted(
            (
                {"name": name, "snapshot-id": ref.snapshot_id}
                for name, ref in refs.items()
                if ref.snapshot_ref_type == SnapshotRefType.TAG
            ),
            key=lambda tag: tag["name"],
        )

    def resolve_tag(self, identifier: str, name: str) -> int:
        """The snapshot id a tag names, or TagNotFound.

        Matched EXACTLY as spelled. The native catalog lowercases tag names on
        the way in, so `MyTag` and `mytag` are one tag there; an Iceberg ref
        name is whatever the engine that wrote it stored, case included, and
        is not opteryx's to normalize - lowercasing here would fail to
        resolve a `Q1_2025` tag that plainly exists and is listed by
        `SHOW SNAPSHOTS`.

        A branch is not resolvable by this path (see the section comment), so
        a branch name raises TagNotFound like any other name that is not a
        tag: opteryx has no way to express "read this branch", and silently
        answering with a branch head would be a different question answered.
        """
        ref = self._load_table(identifier).metadata.refs.get(name)
        if ref is None or ref.snapshot_ref_type != SnapshotRefType.TAG:
            raise TagNotFound(f"Tag not found: {name} on {identifier}")
        return ref.snapshot_id

    def _load_table(self, identifier: str):
        """`load_table`, with pyiceberg's misses translated at the boundary.

        The same translation `load_dataset` performs - kept in one place so a
        tag lookup against a table that does not exist reports the missing
        TABLE rather than a pyiceberg exception type.
        """
        try:
            return self._catalog.load_table(self._to_iceberg_identifier(identifier))
        except (NoSuchTableError, NoSuchNamespaceError) as exc:
            raise DatasetNotFound(identifier) from exc

    def create_dataset(self, identifier: str, schema, properties=None, author=None, location=None):
        """CREATE TABLE: a format-version 2, unpartitioned, unsorted Iceberg
        table with default properties (opteryx_iceberg.writes). Iceberg records
        no author. A `location` is a CTAS's pending writers' - CTAS is not
        supported, so it is refused rather than ignored."""
        from opteryx_catalog.exceptions import DatasetAlreadyExists
        from pyiceberg.exceptions import TableAlreadyExistsError

        from opteryx_iceberg.writes import FORMAT_VERSION
        from opteryx_iceberg.writes import iceberg_schema

        if location is not None:
            raise NotImplementedError(
                "opteryx-iceberg does not support CREATE TABLE ... AS SELECT - CREATE TABLE, "
                "then INSERT INTO ... SELECT."
            )
        if properties:
            raise NotImplementedError(
                f"opteryx-iceberg creates tables with default properties only (got {properties!r})."
            )
        if schema is None:
            raise ValueError(f"opteryx-iceberg: CREATE TABLE {identifier} needs a schema.")
        try:
            self._catalog.create_table(
                self._to_iceberg_identifier(identifier),
                schema=iceberg_schema(schema),
                properties={"format-version": str(FORMAT_VERSION)},
            )
        except TableAlreadyExistsError as exc:
            raise DatasetAlreadyExists(f"Dataset already exists: {identifier}") from exc
        return self.load_dataset(identifier)

    def open_pending_data_file_writer(self, identifier: str, *args, **kwargs):
        # The first call a CTAS makes, before any table exists.
        raise NotImplementedError(
            "opteryx-iceberg does not support CREATE TABLE ... AS SELECT - CREATE TABLE, "
            "then INSERT INTO ... SELECT."
        )

    def list_vector_indexes(self, identifier: str) -> list:
        # Iceberg has no vector indexes; the engine asks before every INSERT.
        return []

    def drop_dataset(self, identifier: str, author: str | None = None) -> None:
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) is read-only - writing is Tier 2 scope."
        )

    def drop_view(self, identifier: str, author: str | None = None) -> None:
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) is read-only - writing is Tier 2 scope."
        )

    def rename_dataset(self, identifier: str, new_identifier: str, author=None) -> None:
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) is read-only - writing is Tier 2 scope."
        )

    def create_collection(self, namespace: str, exists_ok: bool = False, author=None) -> None:
        if exists_ok:
            self._catalog.create_namespace_if_not_exists(self._to_iceberg_namespace(namespace))
        else:
            self._catalog.create_namespace(self._to_iceberg_namespace(namespace))

    def drop_collection(self, namespace: str, author=None) -> None:
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) is read-only - writing is Tier 2 scope."
        )

    @staticmethod
    def _to_iceberg_identifier(identifier: str) -> tuple:
        # Opteryx identifiers are "collection.dataset_name"; pyiceberg wants
        # a namespace.table tuple - "collection" maps onto "namespace".
        return tuple(identifier.split("."))

    @staticmethod
    def _to_iceberg_namespace(namespace: str) -> tuple:
        return tuple(namespace.split("."))


class UnrestrictedIcebergMetastore(IcebergMetastore):
    """IcebergMetastore without `check_tenant_properties`: any catalog type and
    any pyiceberg property, including ones that load code or present ambient
    credentials. ONLY for configuration the deployment's operator writes (a
    self-hosted box's environment, a script, a test) - never for a binding a
    workspace owner can edit. A REST catalog is still `_GuardedRestCatalog`, and
    its files may be at any location the reader supports (OpteryxFileIO)."""

    _file_io = OpteryxFileIO
    _py_io_impl = PY_IO_IMPL

    @staticmethod
    def _rest_catalog(workspace: str, properties: dict):
        # An operator's catalog may be private (a local Lakekeeper, a VPC service).
        return _GuardedRestCatalog(workspace, **properties)

    def __init__(
        self,
        workspace: str,
        catalog_type: str = "rest",
        **properties,
    ):
        self._open(workspace, catalog_type, properties)
