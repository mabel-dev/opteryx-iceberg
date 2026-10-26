"""IcebergDataset: wraps a pyiceberg.table.Table as an opteryx_catalog Dataset.

Read-only (Tier 1). `append` and any other write path raise NotImplementedError.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote
from urllib.parse import urlparse

from draken.draken_native import DrakenType
from opteryx_catalog.catalog.dataset import Datafile
from opteryx_catalog.catalog.dataset import RelationSchema
from opteryx_catalog.catalog.dataset import SchemaColumn
from opteryx_catalog.catalog.metastore import Dataset
from opteryx_catalog.exceptions import SnapshotMissingError
from pyiceberg.types import BinaryType
from pyiceberg.types import BooleanType
from pyiceberg.types import DateType
from pyiceberg.types import DecimalType
from pyiceberg.types import DoubleType
from pyiceberg.types import FixedType
from pyiceberg.types import FloatType
from pyiceberg.types import IntegerType
from pyiceberg.types import ListType
from pyiceberg.types import LongType
from pyiceberg.types import MapType
from pyiceberg.types import StringType
from pyiceberg.types import StructType
from pyiceberg.types import TimeType
from pyiceberg.types import TimestampType
from pyiceberg.types import TimestamptzType
from pyiceberg.types import UUIDType

from opteryx_iceberg.manifests import plan_data_files

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
    # A real type, not text (opteryx-core docs/UUID_TYPE_DESIGN.md): 16 bytes, one meaning in
    # every format. It was VARCHAR before the type existed, which made a table with a uuid
    # column unreadable - the Parquet scan refused a fixed_len_byte_array under a varchar plan.
    UUIDType: "UUID",
    # Unannotated parquet BYTE_ARRAY (e.g. the canonical ClickBench hits.parquet
    # text columns). VARBINARY is what core's parquet reader types the same
    # column as when it reads the file directly, so a query means the same thing
    # against the Iceberg table as against the file. Bounds are the raw bytes.
    BinaryType: "VARBINARY",
}


# The element types of a list that surfaces as an ARRAY, spelled as opteryx-core's own Parquet reader
# types the same column (READ_PARQUET binds array<int32> as ARRAY<INT64>, array<float32> as
# ARRAY<FLOAT64>). Any other list of scalars is refused rather than guessed at.
_ARRAY_ELEMENTS = {
    BooleanType: "BOOLEAN",
    IntegerType: "INT64",
    LongType: "INT64",
    FloatType: "FLOAT64",
    DoubleType: "FLOAT64",
    StringType: "VARCHAR",
}


def _list_bottom(field_type: Any) -> tuple[Any, int]:
    """The element type at the bottom of a list nest, and how many lists deep it is."""
    depth = 0
    while isinstance(field_type, ListType):
        field_type = field_type.element_type
        depth += 1
    return field_type, depth


def _nested_display_type(field_type: Any) -> str:
    """The type a STRUCT / MAP / LIST column binds to (docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.1): JSON
    text (NVARCHAR) for a struct, a map with string keys, and a list with a struct or map at the bottom;
    an ARRAY for a list of a plain scalar. Anything else is refused, naming it."""
    if isinstance(field_type, StructType):
        _check_nested(field_type)
        return "NVARCHAR"
    if isinstance(field_type, MapType):
        _check_nested(field_type)
        return "NVARCHAR"
    bottom, depth = _list_bottom(field_type)
    if isinstance(bottom, (StructType, MapType)):
        _check_nested(bottom)
        return "NVARCHAR"
    for scalar, display in _ARRAY_ELEMENTS.items():
        if isinstance(bottom, scalar):
            if depth == 1:
                return f"ARRAY<{display}>"
            raise NotImplementedError(
                f"opteryx-iceberg does not support a list of lists of {bottom!r} yet: the engine's own "
                "Parquet reader declares a nested ARRAY one level short"
            )
    raise NotImplementedError(f"opteryx-iceberg does not support a list of {bottom!r} yet")


def _check_nested(field_type: Any) -> None:
    """Refuse, naming it, a nested type the JSON rendering cannot hold: a map whose keys are not strings
    (a JSON object's keys are strings)."""
    if isinstance(field_type, MapType):
        if not isinstance(field_type.key_type, StringType):
            raise NotImplementedError(
                f"opteryx-iceberg does not support a map with {field_type.key_type!r} keys: a JSON "
                "object's keys are strings"
            )
        _check_nested(field_type.value_type)
    elif isinstance(field_type, StructType):
        for field in field_type.fields:
            _check_nested(field.field_type)
    elif isinstance(field_type, ListType):
        _check_nested(field_type.element_type)


def _nested_tree(field_id: int, name: str, field_type: Any) -> tuple:
    """The declared shape of a nested column as plain tuples (kind, field id, name, children):
    opteryx-core's NestedField, spelled without importing it. A list's element and a map's key / value
    are named "element" / "key" / "value"; their names are never compared."""
    if isinstance(field_type, StructType):
        return ("struct", field_id, name, tuple(_nested_tree(f.field_id, f.name, f.field_type) for f in field_type.fields))
    if isinstance(field_type, ListType):
        return ("list", field_id, name, (_nested_tree(field_type.element_id, "element", field_type.element_type),))
    if isinstance(field_type, MapType):
        return (
            "map",
            field_id,
            name,
            (
                _nested_tree(field_type.key_id, "key", field_type.key_type),
                _nested_tree(field_type.value_id, "value", field_type.value_type),
            ),
        )
    return ("scalar", field_id, name, ())


@dataclass
class IcebergSchemaColumn(SchemaColumn):
    """A catalog SchemaColumn that also carries the column's declared nested shape (see `_nested_tree`),
    which opteryx-core verifies each data file against."""

    nested: tuple | None = None


def _display_type(field_type: Any) -> str:
    if isinstance(field_type, (StructType, MapType, ListType)):
        return _nested_display_type(field_type)
    if isinstance(field_type, DecimalType):
        return f"DECIMAL({field_type.precision}, {field_type.scale})"
    if isinstance(field_type, FixedType):
        # fixed(L): bytes whose length is L. Avro and ORC data files carry the length and the
        # reader checks every value against it; a Parquet fixed_len_byte_array that is neither a
        # decimal nor a uuid is still refused by core's Parquet reader (loudly).
        return "VARBINARY"
    if isinstance(field_type, TimeType):
        # Held, not unsupported by design (opteryx-core docs/UUID_AND_NESTED_COLUMNS_DESIGN.md UD6
        # required a Parquet check first, and it failed): the engine reads a Parquet TIME column -
        # and CAST(... AS TIME) - as INT64 microseconds, while the Avro and ORC readers produce
        # TIME64 (datetime.time). One Iceberg type must not mean two things by file format.
        raise NotImplementedError(
            "opteryx-iceberg does not support Iceberg type 'time' yet: the engine does not "
            "represent TIME the same way in Parquet and in Avro/ORC data files"
        )
    for iceberg_type, display in _PRIMITIVE_TYPES.items():
        if isinstance(field_type, iceberg_type):
            return display
    raise NotImplementedError(
        f"opteryx-iceberg (Tier 1, read-only) does not support Iceberg type "
        f"{field_type!r} - nested/complex types are out of scope."
    )


# ---------------------------------------------------------------------------
# SHOW SNAPSHOTS FOR
#
# opteryx-core normalizes a commit history through
# `opteryx.models.snapshot_history.normalize_snapshot`, which reads its fields
# off the snapshot BY ATTRIBUTE - it was written against opteryx_catalog's own
# `Snapshot` dataclass and takes it straight in. A pyiceberg `Snapshot` is not
# that shape: it has snapshot_id/parent_snapshot_id/sequence_number/
# timestamp_ms/schema_id, and no `operation_type`, `author`, `user_created` or
# `commit_message` at all - so handing one over raises AttributeError and the
# statement fails outright.
#
# The adaptation belongs HERE, for the same reason `scan` builds
# opteryx_catalog's `Datafile` rather than teaching opteryx-core about
# pyiceberg: the Dataset boundary is where a backend meets the engine's
# vocabulary. normalize_snapshot's own docstring says the connector normalizes
# "so a second connector with a commit log answers the same statement by
# producing the same dicts" - this is that.
#
# Two mappings are NOT identity and are the whole reason this class exists
# rather than a `getattr` default in the engine:
#
# 1. `summary` values are STRINGS in Iceberg ("609"), and every counter column
#    in the output is INT64. Passed through raw they reach a typed vector
#    builder as text. `_summary_int` coerces, and a value that is not an
#    integer becomes None (unknown) rather than 0 - claiming a commit added
#    nothing is a worse answer than admitting the counter is unreadable.
#
# 2. Iceberg spells the deleted-bytes counter `removed-files-size`; opteryx's
#    column is `deleted-files-size`. The other eight counters share a spelling
#    with the Iceberg spec exactly. Mapped explicitly below, because a silent
#    miss here is an all-null column rather than an error.
#
# Fields Iceberg genuinely does not record are None, never invented:
# `author` and `commit_message` have no Iceberg equivalent (the spec has no
# notion of who committed or why), and `user_created` is an opteryx
# distinction between user and system commits that Iceberg does not draw.
# None reads as "unknown" in the output, which is the truth.

# opteryx column name -> Iceberg summary key. Identity for eight of nine; the
# ninth is the removed/deleted spelling difference.
_SUMMARY_KEY_MAP = {
    "added-records": "added-records",
    "added-data-files": "added-data-files",
    "added-files-size": "added-files-size",
    "deleted-records": "deleted-records",
    "deleted-data-files": "deleted-data-files",
    "deleted-files-size": "removed-files-size",
    "total-records": "total-records",
    "total-data-files": "total-data-files",
    "total-files-size": "total-files-size",
}


def _summary_int(value: Any) -> int | None:
    """An Iceberg summary counter as an int, or None if it is not readable."""
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class IcebergSnapshot:
    """One pyiceberg snapshot in the shape opteryx-core reads snapshots in.

    Carries the pyiceberg snapshot's own identity fields through unchanged and
    supplies the four opteryx fields Iceberg has no equivalent for as None.
    `summary` is a plain dict keyed the way opteryx spells the counters, so
    `normalize_snapshot`'s `summary.get(...)` lookups land.
    """

    __slots__ = (
        "_snapshot",
        "snapshot_id",
        "parent_snapshot_id",
        "sequence_number",
        "timestamp_ms",
        "schema_id",
        "manifest_list",
        "operation_type",
        "author",
        "user_created",
        "commit_message",
        "summary",
    )

    def __init__(self, snapshot: Any):
        self._snapshot = snapshot
        self.snapshot_id = snapshot.snapshot_id
        self.parent_snapshot_id = snapshot.parent_snapshot_id
        self.sequence_number = snapshot.sequence_number
        self.timestamp_ms = snapshot.timestamp_ms
        # Iceberg's manifest list: written once per snapshot and never rewritten,
        # so it names exactly one manifest - opteryx-core caches the decoded
        # manifest under it (see IcebergDataset.manifest_bytes).
        self.manifest_list = snapshot.manifest_list
        # `schema_id` is an INT in Iceberg and a VARCHAR column in opteryx's
        # output (opteryx_catalog spells its schema ids as strings), so it is
        # stringified here rather than at the vector builder, which would
        # reject the int outright. Safe against the other reader of this
        # attribute: `_resolve_snapshot` passes it to `IcebergDataset.schema`,
        # which ignores the argument entirely and returns the table's current
        # schema (Tier 1 has no historical-schema lookup).
        self.schema_id = None if snapshot.schema_id is None else str(snapshot.schema_id)

        summary = snapshot.summary
        # `Summary.operation` is an `Operation` enum; the column is VARCHAR.
        # `.value` gives the spec's lowercase name ("append", "overwrite", ...)
        # rather than "Operation.APPEND".
        operation = getattr(summary, "operation", None)
        self.operation_type = getattr(operation, "value", operation)

        # No Iceberg equivalent - see the module comment above.
        self.author = None
        self.user_created = None
        self.commit_message = None

        self.summary = {
            column: _summary_int(summary.get(key) if summary is not None else None)
            for column, key in _SUMMARY_KEY_MAP.items()
        }

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return f"IcebergSnapshot(snapshot_id={self.snapshot_id})"


class IcebergDataset(Dataset):
    # Iceberg's lower/upper bounds are converted to the ordinal int64 keys
    # opteryx-catalog's own stats builder writes (`_ordinal_bound`), so the
    # manifest this serves is the opteryx manifest parquet. See the contract on
    # `Dataset.bounds_are_ordinal` for what mis-declaring this does.
    bounds_are_ordinal = True

    def __init__(self, identifier: str, table, io):
        """`io` is the FileIO manifests are read through - always an
        OpteryxFileIO, never `table.io` (see IcebergMetastore.load_dataset)."""
        self.identifier = identifier
        self._table = table
        self._io = io

    @property
    def metadata(self) -> Any:
        return self._table.metadata

    def snapshots(self) -> Iterable[Any]:
        """The full commit history, as IcebergSnapshot (see that class).

        Order is pyiceberg's; opteryx-core sorts newest-first itself, so this
        deliberately does not.
        """
        return [IcebergSnapshot(snap) for snap in self._table.snapshots()]

    def snapshot(self, snapshot_id: int | None = None) -> Any | None:
        """One snapshot by id, or the current one. Also IcebergSnapshot.

        Wrapped for the same reason `snapshots` is, and so that a caller
        reading `.schema_id` off a targeted lookup and off the history sees one
        type rather than two.
        """
        if snapshot_id is None:
            current = self._table.current_snapshot()
            return None if current is None else IcebergSnapshot(current)
        for snap in self._table.snapshots():
            if snap.snapshot_id == snapshot_id:
                return IcebergSnapshot(snap)
        return None

    def previous_user_snapshot(self) -> IcebergSnapshot | None:
        """The snapshot `VERSION AS OF PREVIOUS` and the `previous` tag name.

        On Iceberg this is the LITERAL PARENT of the current snapshot. The
        native `SimpleDataset.previous_user_snapshot` walks past maintenance
        commits (compaction, statistics refresh) to the previous version of the
        DATA; this does not. Iceberg carries no `user_created`, so a parent that
        is a `replace` commit (files rewritten, rows unchanged) is returned as
        is, and PREVIOUS then reads the same data as an unqualified read.

        None when the current snapshot has no parent (the first commit) or the
        table has no snapshot. A parent id that names a snapshot the table no
        longer retains (expired) raises, as the native walk does: there IS a
        previous version, it just cannot be read, and None would say otherwise.
        """
        current = self.snapshot()
        if current is None or current.parent_snapshot_id is None:
            return None
        parent = self.snapshot(current.parent_snapshot_id)
        if parent is None:
            raise SnapshotMissingError(
                f"The version before snapshot {current.snapshot_id} of "
                f"{self.identifier} has expired and can no longer be read."
            )
        return parent

    def schema(self, schema_id: str | int | None = None) -> RelationSchema | None:
        """The table's schema, or the historical schema `schema_id` names.

        Resolving `schema_id` matters for TIME TRAVEL and nothing else: reading
        an old snapshot through the CURRENT schema reports columns that did not
        exist yet. `SELECT *` at a pre-`ADD COLUMN` snapshot listed the added
        column, with no error and no way for the caller to tell.

        Iceberg keeps every schema the table has had (`Table.schemas()`, keyed
        by id) and every snapshot records the id it was written under, so the
        right schema is a lookup. An id that is not in that map falls back to
        the current schema: a snapshot naming a schema the table no longer
        retains is a damaged table, and answering with today's columns is the
        same thing this did for every snapshot before - degraded, not worse.
        """
        iceberg_schema = None
        if schema_id is not None:
            # IcebergSnapshot stringifies schema_id for the VARCHAR output
            # column, so it arrives here as "0" as often as 0.
            try:
                wanted = int(schema_id)
            except (TypeError, ValueError):
                wanted = None
            if wanted is not None:
                iceberg_schema = self._table.schemas().get(wanted)
        if iceberg_schema is None:
            iceberg_schema = self._table.schema()
        columns = [
            IcebergSchemaColumn(
                name=field.name,
                type=_display_type(field.field_type),
                nullable=not field.required,
                id=field.field_id,
                nested=_nested_tree(field.field_id, field.name, field.field_type)
                if isinstance(field.field_type, (StructType, ListType, MapType))
                else None,
            )
            for field in iceberg_schema.fields
        ]
        return RelationSchema(name=self.identifier, columns=columns)

    def scan(
        self, row_filter=None, snapshot_id: int | None = None, row_limit: int | None = None
    ) -> Iterable[Datafile]:
        for entry in self._manifest_entries(snapshot_id):
            yield Datafile(entry=entry)

    def manifest_bytes(self, snapshot_id: int | None = None) -> bytes | None:
        """The snapshot's files as the opteryx manifest parquet (see
        `Dataset.manifest_bytes`), built from Iceberg's own manifests. Built on
        every call: opteryx-core caches the decoded manifest by the snapshot's
        `manifest_list` (Iceberg's manifest list, written once per snapshot), so
        this runs once per snapshot per process, not once per query."""
        from opteryx_catalog.catalog.manifest import encode_parquet_manifest

        snap = self.snapshot(snapshot_id)
        if snap is None or not snap.manifest_list:
            return None
        return encode_parquet_manifest(list(self._manifest_entries(snap.snapshot_id)))

    def _manifest_entries(self, snapshot_id: int | None) -> Iterable[dict]:
        """One opteryx manifest entry per data file of the snapshot: counts,
        sizes, and per field id the ordinal min/max bound and null count.

        The snapshot's manifest list and manifests are read with rugo's Avro
        reader (`opteryx_iceberg.manifests`), not pyiceberg's `plan_files()`."""
        metadata = self._table.metadata
        snapshot = (
            metadata.current_snapshot()
            if snapshot_id is None
            else metadata.snapshot_by_id(snapshot_id)
        )
        if snapshot is None:
            return
        iceberg_schema = self._table.schema()
        ordinal_types = {}
        for field in iceberg_schema.fields:
            column_type = _column_type(field.field_type)
            if column_type is not None:
                ordinal_types[field.field_id] = column_type.physical

        for data_file in plan_data_files(self._io, snapshot, self.identifier, ordinal_types):
            yield {
                "file_path": _reader_path(data_file.file_path),
                "file_format": data_file.file_format,
                "record_count": data_file.record_count,
                "file_size_in_bytes": data_file.file_size_in_bytes,
                "field_ids": data_file.field_ids,
                "min_values": data_file.min_values,
                "max_values": data_file.max_values,
                "null_counts": data_file.null_counts,
                # No min_k_hashes/histogram_counts - Iceberg manifests
                # carry no equivalent sketch stats.
            }

    def append(self, table):
        raise NotImplementedError(
            "opteryx-iceberg (Tier 1) is read-only - writing is Tier 2 scope."
        )


def _reader_path(file_path: str) -> str:
    """Iceberg's location -> the form opteryx-core's reader can open.

    Iceberg records every datafile location as a URI, so a local warehouse
    yields "file:///var/.../x.parquet". Opteryx routes on the scheme prefix
    (opteryx/connectors/io_systems/__init__.py maps "gs", "http(s)", "file"
    and "" to a filesystem) but its local filesystem is the plain-OS-path
    branch: MemoryMappedFile mmaps the string as given, and rugo's C++ reader
    raises "RuntimeError: Cannot open file: file:///..." on a path that still
    carries the scheme. Only "file://" is rewritten - "gs://" and "s3://"
    locations are the form their readers want and are passed through
    untouched.

    The path component is percent-decoded, since that is what a URI's path is:
    a warehouse directory containing a space is written as "%20" by pyiceberg
    and must reach the reader as a space. A "file://host/path" authority other
    than "localhost" is a remote share this reader cannot open, so it is left
    alone rather than silently reinterpreted as a local path.
    """
    if not file_path.startswith("file://"):
        return file_path
    parsed = urlparse(file_path)
    if parsed.netloc not in ("", "localhost"):
        return file_path
    return unquote(parsed.path)


def _column_type(field_type: Any) -> Any:
    """The ColumnType opteryx-core binds an Iceberg column to (or None for a type
    `_display_type` does not map, whose bounds are then left out). Its physical
    type decides how the column's Iceberg bounds are turned into ordinal keys
    (opteryx-core src/cpp/planner/iceberg_bounds.hpp)."""
    from opteryx.types.logical_type import try_parse_column_type

    if isinstance(field_type, (UUIDType, TimeType, FixedType)):
        # No ordinal key for these (UUID: opteryx-core UUID design U14 - no min/max pruning yet;
        # time and fixed: not mapped to an ordinal here): no bound. No statistics is slower,
        # never wrong.
        return None
    for iceberg_type in (*_PRIMITIVE_TYPES, DecimalType):
        if isinstance(field_type, iceberg_type):
            column_type = try_parse_column_type(_display_type(field_type))
            if column_type.physical == DrakenType.DECIMAL128:
                # A decimal wider than 18 digits: draken has no ordinal key for
                # DECIMAL128, so its bounds are left out rather than failing the
                # scan - no pruning on the column, never a wrong answer.
                return None
            return column_type
    return None

