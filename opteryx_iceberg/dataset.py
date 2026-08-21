"""IcebergDataset: wraps a pyiceberg.table.Table as an opteryx_catalog Dataset.

Read-only (Tier 1). `append` and any other write path raise NotImplementedError.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from opteryx_catalog.catalog.dataset import Datafile
from opteryx_catalog.catalog.dataset import RelationSchema
from opteryx_catalog.catalog.dataset import SchemaColumn
from opteryx_catalog.catalog.metastore import Dataset
from pyiceberg.conversions import from_bytes
from pyiceberg.types import BooleanType
from pyiceberg.types import DateType
from pyiceberg.types import DecimalType
from pyiceberg.types import DoubleType
from pyiceberg.types import FloatType
from pyiceberg.types import IntegerType
from pyiceberg.types import LongType
from pyiceberg.types import StringType
from pyiceberg.types import TimestampType
from pyiceberg.types import TimestamptzType
from pyiceberg.types import UUIDType

# Iceberg primitive -> Opteryx display type string, the vocabulary
# opteryx.types.logical_type.try_parse_column_type accepts (see
# opteryx_catalog.catalog.dataset._stored_type_display for the native path's
# equivalent). Nested types (Struct/Map/List) are not in Tier 1's scope -
# _display_type raises rather than silently misrepresenting them as VARCHAR.
# Widths are NOT interchangeable here, even though INTEGER/BIGINT and
# DOUBLE/FLOAT are interchangeable *names* in SQL. The type this returns is what
# opteryx-core binds the column to, and the reader then reads the parquet column
# at the bound type's width: declaring Iceberg's 4-byte IntegerType as INT64, or
# its 4-byte FloatType as FLOAT64, makes the reader take 8 bytes per value from a
# 4-byte column. That is not a rounding difference - the values are garbage, they
# vary run to run, and predicates over the column return an arbitrary and
# unstable number of rows with no error. Map each Iceberg type to the Opteryx
# type of the SAME physical width and let the engine widen if it wants to.
_PRIMITIVE_TYPES = {
    BooleanType: "BOOLEAN",
    IntegerType: "INT32",
    LongType: "INT64",
    FloatType: "FLOAT32",
    DoubleType: "FLOAT64",
    DateType: "DATE",
    TimestampType: "TIMESTAMP",
    TimestamptzType: "TIMESTAMP",
    StringType: "VARCHAR",
    UUIDType: "VARCHAR",
}


def _display_type(field_type: Any) -> str:
    if isinstance(field_type, DecimalType):
        return f"DECIMAL({field_type.precision}, {field_type.scale})"
    for iceberg_type, display in _PRIMITIVE_TYPES.items():
        if isinstance(field_type, iceberg_type):
            return display
    raise NotImplementedError(
        f"opteryx-iceberg (Tier 1, read-only) does not support Iceberg type "
        f"{field_type!r} - nested/complex types are out of scope."
    )


class IcebergDataset(Dataset):
    # `_decode_bound` hands back what `from_bytes` decodes -- a real `str` for
    # a StringType column, a real `float` for a DoubleType -- not the ordinal
    # int64 keys opteryx-catalog's own stats builder writes. See the contract
    # on `Dataset.bounds_are_ordinal` for what mis-declaring this does.
    bounds_are_ordinal = False

    def __init__(self, identifier: str, table):
        self.identifier = identifier
        self._table = table

    @property
    def metadata(self) -> Any:
        return self._table.metadata

    def snapshots(self) -> Iterable[Any]:
        return self._table.snapshots()

    def snapshot(self, snapshot_id: int | None = None) -> Any | None:
        if snapshot_id is None:
            return self._table.current_snapshot()
        for snap in self._table.snapshots():
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def schema(self, schema_id: str | None = None) -> RelationSchema | None:
        # pyiceberg tables carry one live schema per snapshot lineage;
        # schema_id (opteryx-catalog's historical-schema lookup) has no
        # equivalent here - Tier 1 always returns the table's current schema.
        iceberg_schema = self._table.schema()
        columns = [
            SchemaColumn(
                name=field.name,
                type=_display_type(field.field_type),
                nullable=not field.required,
                id=field.field_id,
            )
            for field in iceberg_schema.fields
        ]
        return RelationSchema(name=self.identifier, columns=columns)

    def scan(
        self, row_filter=None, snapshot_id: int | None = None, row_limit: int | None = None
    ) -> Iterable[Datafile]:
        iceberg_schema = self._table.schema()
        field_by_id = {f.field_id: f for f in iceberg_schema.fields}

        table_scan = self._table.scan(snapshot_id=snapshot_id)
        for task in table_scan.plan_files():
            data_file = task.file
            field_ids = sorted(
                set(data_file.lower_bounds or {}) | set(data_file.upper_bounds or {})
            )
            min_values = [
                _decode_bound(data_file.lower_bounds, fid, field_by_id) for fid in field_ids
            ]
            max_values = [
                _decode_bound(data_file.upper_bounds, fid, field_by_id) for fid in field_ids
            ]
            null_counts = [
                (data_file.null_value_counts or {}).get(fid) for fid in field_ids
            ]
            yield Datafile(
                entry={
                    "file_path": data_file.file_path,
                    "record_count": data_file.record_count,
                    "file_size_in_bytes": data_file.file_size_in_bytes,
                    "field_ids": field_ids,
                    "min_values": min_values,
                    "max_values": max_values,
                    "null_counts": null_counts,
                    # No min_k_hashes/histogram_counts - Iceberg manifests
                    # carry no equivalent sketch stats. opteryx-core's
                    # pruning already falls back cleanly to "no sketches".
                }
            )

    def manifest_sketch_vectors(self, snapshot_id: int | None = None) -> dict:
        """Always empty: Iceberg manifests carry no sketch columns.

        opteryx-core probes for this accessor with `getattr(table, ...)` and
        treats its absence as "the catalog is too old to expose native sketch
        vectors", logging a warning that tells the operator to upgrade
        opteryx_catalog. For an Iceberg-backed workspace that advice is
        unactionable - no version of opteryx_catalog puts NDV/histogram
        sketches into an Iceberg manifest, because the Iceberg spec has no
        field to hold them (see `scan`, which likewise emits no
        min_k_hashes/histogram_counts).

        `{}` is not a stub to silence the warning - it is the accessor's own
        answer for "this snapshot has no sketch columns", identical to what
        opteryx_catalog returns for a snapshot with no manifest, and identical
        to the value opteryx-core's fallback branch assigns anyway. Defining it
        changes no query result; it only stops the engine from misreporting an
        inherent property of the format as a stale dependency.
        """
        return {}

    def append(self, table):
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) is read-only - writing is Tier 2 scope."
        )


def _decode_bound(bounds: dict | None, field_id: int, field_by_id: dict) -> Any:
    if not bounds or field_id not in bounds:
        return None
    field = field_by_id.get(field_id)
    if field is None:
        return None
    return from_bytes(field.field_type, bounds[field_id])
