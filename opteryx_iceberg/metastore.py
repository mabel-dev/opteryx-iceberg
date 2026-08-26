"""IcebergMetastore: wraps pyiceberg's own catalog loader (REST/SQL/Hive/Glue,
whichever `catalog_type`/`uri` selects) as an opteryx_catalog Metastore.

Read-only (Tier 1). Registered per-workspace via the existing
`opteryx.connectors.register_workspace(prefix, OpteryxConnector,
catalog=IcebergMetastore, workspace=..., **iceberg_properties)` seam - no new
routing mechanism needed.

Duck-type surface: OpteryxConnector calls a wider set of catalog methods than
the formal `Metastore` ABC (see opteryx-core/opteryx/connectors/
opteryx_connector.py). `load_dataset`, `get_relation`, `dataset_exists`,
`list_datasets`, and `load_view` are implemented for real, since they sit on
the plain-SELECT read path. Everything DDL/write/materialized-view/trigger-
shaped raises NotImplementedError - none of it applies to an external,
read-only Iceberg source, and Tier 2 is where writes belong.
"""

from __future__ import annotations

from collections.abc import Iterable

from opteryx_catalog.catalog.metastore import Metastore
from opteryx_catalog.exceptions import DatasetNotFound
from opteryx_catalog.exceptions import TagNotFound
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table.refs import SnapshotRefType

from opteryx_iceberg.dataset import IcebergDataset
from opteryx_iceberg.fileio import IcebergFileIO


class IcebergMetastore(Metastore):
    def __init__(
        self,
        workspace: str,
        catalog_type: str = "rest",
        **properties,
    ):
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
                "glue", ...) - passed through as `properties["type"]`.
            **properties: forwarded VERBATIM to
                `pyiceberg.catalog.load_catalog` (e.g. `uri`, `warehouse`,
                `token`, `credential`, `auth={...}`). Values may be
                arbitrarily nested - pyiceberg's own config shapes, such
                as `auth={"type": "google", "google": {"scopes": [...]}}`,
                are used directly.

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
        self.workspace = workspace
        self._catalog = load_catalog(workspace, type=catalog_type, **properties)
        self.io = IcebergFileIO(properties)

    def load_dataset(self, identifier: str, load_history: bool = False) -> IcebergDataset:
        return IcebergDataset(identifier, self._load_table(identifier))

    def load_view(self, identifier: str):
        # No Iceberg view-spec support in Tier 1. Callers (e.g.
        # OpteryxConnector._try_load_view) catch any Exception broadly and
        # treat it as "not a view", so raising here is correct, not a stub.
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) does not support Iceberg views."
        )

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

    def create_dataset(self, identifier: str, schema, properties=None, author=None):
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) is read-only - writing is Tier 2 scope."
        )

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
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) is read-only - writing is Tier 2 scope."
        )

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
