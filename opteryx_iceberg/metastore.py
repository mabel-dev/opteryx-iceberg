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
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError
from pyiceberg.exceptions import NoSuchTableError

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
                resolver) - kept for parity with `OpteryxCatalog`'s
                constructor shape, not currently used to namespace
                pyiceberg calls.
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
        try:
            table = self._catalog.load_table(self._to_iceberg_identifier(identifier))
        except (NoSuchTableError, NoSuchNamespaceError) as exc:
            raise DatasetNotFound(identifier) from exc
        return IcebergDataset(identifier, table)

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
        return [
            ".".join(identifier) for identifier in self._catalog.list_tables(namespace)
        ]

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
