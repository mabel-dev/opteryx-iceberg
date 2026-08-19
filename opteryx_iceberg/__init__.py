"""Read-only Apache Iceberg backend for opteryx-catalog (Tier 1).

Lets a workspace be registered against an external Iceberg catalog (REST,
SQL, Hive, Glue - whatever pyiceberg's own loader supports) instead of the
native Firestore+GCS backend, via the existing
`opteryx.connectors.register_workspace(prefix, OpteryxConnector,
catalog=IcebergMetastore, workspace=..., **properties)` seam. Optional add-on,
kept out of opteryx-catalog/opteryx-core's dependency tree - see the package
README for why.
"""

from .dataset import IcebergDataset
from .fileio import IcebergFileIO
from .metastore import IcebergMetastore

__all__ = [
    "IcebergDataset",
    "IcebergFileIO",
    "IcebergMetastore",
]
