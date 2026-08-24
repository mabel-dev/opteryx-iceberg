"""IcebergDataset: wraps a pyiceberg.table.Table as an opteryx_catalog Dataset.

Read-only (Tier 1). `append` and any other write path raise NotImplementedError.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any
from urllib.parse import unquote
from urllib.parse import urlparse

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
            _reject_merge_on_read(task, self.identifier)
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
                    "file_path": _reader_path(data_file.file_path),
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


def _reject_merge_on_read(task: Any, identifier: str) -> None:
    """Refuse a scan task carrying Iceberg delete files.

    Iceberg v2 has two ways to express a delete. COPY-ON-WRITE rewrites the
    data file without the deleted rows, and the rows are simply gone - that is
    what pyiceberg itself writes, and it needs nothing from us. MERGE-ON-READ
    instead leaves the data file alone and commits a DELETE FILE beside it
    (positional: "rows 3 and 7 of this file are dead"; equality: "any row where
    id = 42 is dead"), and the reader is required to subtract them at scan
    time.

    This package does not do that subtraction. Handing the data file over as-is
    would return every deleted row as live data - the wrong answer, silently,
    with no error and nothing in the result to hint at it. Refusing is strictly
    better: a query that fails is a query nobody acts on.

    This is NOT reachable from a table pyiceberg wrote. pyiceberg has no
    merge-on-read write path at all ("Merge on read is not yet supported,
    falling back to copy-on-write"), so the local SqlCatalog suite cannot
    construct the condition - which is exactly why the guard is here rather
    than left to be noticed. Spark, Flink and Trino DO write merge-on-read
    deletes, and a remote catalog is usually one of those writing.

    Lifting this means implementing delete application (positional and
    equality), which is a different and much larger piece of work.
    """
    delete_files = getattr(task, "delete_files", None)
    if delete_files:
        raise NotImplementedError(
            f"opteryx-iceberg (Tier 1, read-only) cannot read {identifier!r}: it "
            f"has merge-on-read delete files ({len(delete_files)} on one data "
            "file), and reading it without applying them would return deleted "
            "rows as live data. Only copy-on-write tables are supported."
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


def _decode_bound(bounds: dict | None, field_id: int, field_by_id: dict) -> Any:
    if not bounds or field_id not in bounds:
        return None
    field = field_by_id.get(field_id)
    if field is None:
        return None
    return from_bytes(field.field_type, bounds[field_id])
