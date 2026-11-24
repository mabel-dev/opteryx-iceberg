"""Tier 2: CREATE SCHEMA, CREATE TABLE and INSERT into an Iceberg table.

Every file is written by rugo: data files by its Parquet writer (through
opteryx_catalog's `open_data_file_writer_at`, with the Iceberg field-ids in
the Parquet schema), manifests and manifest lists by its Avro writer. pyiceberg
is only the catalog client: `create_namespace`, `create_table`, and the one
`commit_table` that adds the snapshot.

An INSERT is a fast append, as the Iceberg spec defines it: one new manifest
holding the statement's files as ADDED entries, and a manifest list that is
that manifest plus every manifest of the parent snapshot, carried over as it
was. The commit asserts the branch still points at the parent, so a lost race
is a refused commit (`SnapshotRaceError`), never a silent overwrite.

Scope: format-version 2, unpartitioned, unsorted tables with default
properties - what CREATE TABLE here creates. Anything else is refused by name.
"""

from __future__ import annotations

import base64
import json
import struct
import time
import uuid
from typing import Any

from draken.draken_native import DrakenType

FORMAT_VERSION = 2

# rugo's Parquet writer option that stamps each column's Iceberg field-id into
# the file's schema - what every Iceberg reader matches columns by.
PARQUET_FIELD_IDS_OPTION = "field_ids"

# Opteryx physical type -> pyiceberg type, for the types this writer maps
# exactly. Anything else is refused at CREATE TABLE, never written in another
# form.
def _iceberg_type(column_type: Any, name: str):
    from pyiceberg.types import BinaryType
    from pyiceberg.types import BooleanType
    from pyiceberg.types import DateType
    from pyiceberg.types import DecimalType
    from pyiceberg.types import DoubleType
    from pyiceberg.types import FloatType
    from pyiceberg.types import IntegerType
    from pyiceberg.types import LongType
    from pyiceberg.types import StringType
    from pyiceberg.types import TimestampType
    from pyiceberg.types import UUIDType

    physical = column_type.physical
    simple = {
        DrakenType.BOOL: BooleanType,
        DrakenType.INT32: IntegerType,
        DrakenType.INT64: LongType,
        DrakenType.FLOAT32: FloatType,
        DrakenType.FLOAT64: DoubleType,
        DrakenType.DATE32: DateType,
        DrakenType.VARCHAR: StringType,
        DrakenType.VARBINARY: BinaryType,
        DrakenType.UUID: UUIDType,
    }
    if physical in simple:
        return simple[physical]()
    if physical == DrakenType.TIMESTAMP64:
        logical = column_type.logical
        if logical.unit.name != "MICROSECONDS" or logical.offset_minutes != 0:
            raise NotImplementedError(
                f"Iceberg column {name!r}: only microsecond TIMESTAMP without a time zone "
                f"is written (got unit {logical.unit.name}, offset {logical.offset_minutes})."
            )
        return TimestampType()
    if physical == DrakenType.DECIMAL:
        logical = column_type.logical
        return DecimalType(logical.precision, logical.scale)
    raise NotImplementedError(
        f"Iceberg column {name!r}: type {column_type!r} has no exact Iceberg type this "
        "writer maps (BOOLEAN, INTEGER, BIGINT, REAL, DOUBLE, DATE, TIMESTAMP, VARCHAR, "
        "VARBINARY, UUID, DECIMAL(p<=18))."
    )


def iceberg_schema(schema: Any):
    """The pyiceberg Schema for an engine RelationSchema (columns with .name,
    .column_type, .nullable). Field ids are 1..N in column order; the catalog
    may re-assign them, so callers read the created table's schema back."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField

    return Schema(
        *[
            NestedField(
                field_id=index,
                name=column.name,
                field_type=_iceberg_type(column.column_type, column.name),
                required=not column.nullable,
            )
            for index, column in enumerate(schema.columns, start=1)
        ]
    )


# ── bounds: the engine's ordinal keys -> Iceberg single-value bytes ──────────

_SIGN = 0x8000000000000000
_MASK64 = 0xFFFFFFFFFFFFFFFF


def _f64_from_ordinal(key: int) -> float:
    """Invert draken/ops/ordinalize.h `ordinalize_scalar_f64`."""
    bits = key & _MASK64
    if bits & _SIGN:
        bits = _SIGN | (~bits & (_SIGN - 1))
    return struct.unpack("<d", struct.pack("<Q", bits))[0]


def _decimal_bytes(unscaled: int) -> bytes:
    """Two's-complement big-endian, minimum bytes (the Iceberg spec's form)."""
    length = max(1, (unscaled + (unscaled < 0)).bit_length() // 8 + 1)
    return unscaled.to_bytes(length, "big", signed=True)


def _bound_bytes(physical, key: int) -> bytes | None:
    """The Iceberg bound for ordinal `key`, or None where none can be written.

    Integers, dates, timestamps and DECIMAL keys ARE the value, and floats
    invert exactly. A NaN is never a bound (Iceberg spec). Strings, binaries
    and UUIDs have no bound: their ordinal key is an 8-byte prefix shifted
    right by one, and a valid upper bound cannot be rebuilt from it.
    """
    if physical in (DrakenType.INT32, DrakenType.DATE32):
        return struct.pack("<i", key)
    if physical in (DrakenType.INT64, DrakenType.TIMESTAMP64):
        return struct.pack("<q", key)
    if physical == DrakenType.BOOL:
        return struct.pack("<B", key)
    if physical == DrakenType.DECIMAL:
        return _decimal_bytes(key)
    if physical in (DrakenType.FLOAT32, DrakenType.FLOAT64):
        value = _f64_from_ordinal(key)
        if value != value:
            return None
        return struct.pack("<f" if physical == DrakenType.FLOAT32 else "<d", value)
    # shortcut: no string/binary/uuid bounds (lossy 8-byte ordinal prefix), so
    # no pruning on them by any reader; upgrade by carrying real string bounds
    # out of FileStats.
    return None


def _map(pairs: list) -> list:
    return [{"key": key, "value": value} for key, value in pairs]


def data_file_json(row: dict, physical_by_field_id: dict) -> dict:
    """One manifest_entry.data_file for an engine manifest row (the files'
    native statistics, keyed by Iceberg field-id)."""
    lower, upper, nulls, values = [], [], [], []
    record_count = row["record_count"]
    field_ids = row.get("field_ids") or []
    mins = row.get("min_values") or [None] * len(field_ids)
    maxs = row.get("max_values") or [None] * len(field_ids)
    null_counts = row.get("null_counts") or [None] * len(field_ids)
    for field_id, low, high, null_count in zip(field_ids, mins, maxs, null_counts, strict=True):
        physical = physical_by_field_id.get(field_id)
        if physical is None:
            raise ValueError(
                f"Iceberg INSERT: file {row['file_path']!r} has statistics for field id "
                f"{field_id}, which is not a column of the table."
            )
        values.append((field_id, record_count))
        if null_count is not None:
            nulls.append((field_id, null_count))
        all_null = null_count is not None and null_count == record_count
        if all_null:
            continue
        if low is not None and (encoded := _bound_bytes(physical, low)) is not None:
            lower.append((field_id, base64.b64encode(encoded).decode("ascii")))
        if high is not None and (encoded := _bound_bytes(physical, high)) is not None:
            upper.append((field_id, base64.b64encode(encoded).decode("ascii")))
    return {
        "content": 0,
        "file_path": row["file_path"],
        "file_format": "PARQUET",
        "partition": {},
        "record_count": record_count,
        "file_size_in_bytes": row["file_size_in_bytes"],
        "column_sizes": None,
        "value_counts": _map(values),
        "null_value_counts": _map(nulls),
        "nan_value_counts": None,
        "lower_bounds": _map(lower),
        "upper_bounds": _map(upper),
        "key_metadata": None,
        "split_offsets": None,
        "equality_ids": None,
        "sort_order_id": None,
    }


# ── Avro schemas (Iceberg spec, format-version 2) ────────────────────────────


def _int_map(name: str, field_id: int, key_id: int, value_id: int, value_type: str) -> dict:
    return {
        "name": name,
        "type": [
            "null",
            {
                "type": "array",
                "logicalType": "map",
                "items": {
                    "type": "record",
                    "name": f"k{key_id}_v{value_id}",
                    "fields": [
                        {"name": "key", "type": "int", "field-id": key_id},
                        {"name": "value", "type": value_type, "field-id": value_id},
                    ],
                },
            },
        ],
        "default": None,
        "field-id": field_id,
    }


def _optional(name: str, avro_type: Any, field_id: int) -> dict:
    return {"name": name, "type": ["null", avro_type], "default": None, "field-id": field_id}


MANIFEST_ENTRY_SCHEMA = {
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {"name": "status", "type": "int", "field-id": 0},
        _optional("snapshot_id", "long", 1),
        _optional("sequence_number", "long", 3),
        _optional("file_sequence_number", "long", 4),
        {
            "name": "data_file",
            "field-id": 2,
            "type": {
                "type": "record",
                "name": "r2",
                "fields": [
                    {"name": "content", "type": "int", "field-id": 134},
                    {"name": "file_path", "type": "string", "field-id": 100},
                    {"name": "file_format", "type": "string", "field-id": 101},
                    {
                        "name": "partition",
                        "field-id": 102,
                        "type": {"type": "record", "name": "r102", "fields": []},
                    },
                    {"name": "record_count", "type": "long", "field-id": 103},
                    {"name": "file_size_in_bytes", "type": "long", "field-id": 104},
                    _int_map("column_sizes", 108, 117, 118, "long"),
                    _int_map("value_counts", 109, 119, 120, "long"),
                    _int_map("null_value_counts", 110, 121, 122, "long"),
                    _int_map("nan_value_counts", 137, 138, 139, "long"),
                    _int_map("lower_bounds", 125, 126, 127, "bytes"),
                    _int_map("upper_bounds", 128, 129, 130, "bytes"),
                    _optional("key_metadata", "bytes", 131),
                    _optional(
                        "split_offsets",
                        {"type": "array", "items": "long", "element-id": 133},
                        132,
                    ),
                    _optional(
                        "equality_ids",
                        {"type": "array", "items": "int", "element-id": 136},
                        135,
                    ),
                    _optional("sort_order_id", "int", 140),
                ],
            },
        },
    ],
}

MANIFEST_FILE_SCHEMA = {
    "type": "record",
    "name": "manifest_file",
    "fields": [
        {"name": "manifest_path", "type": "string", "field-id": 500},
        {"name": "manifest_length", "type": "long", "field-id": 501},
        {"name": "partition_spec_id", "type": "int", "field-id": 502},
        {"name": "content", "type": "int", "field-id": 517},
        {"name": "sequence_number", "type": "long", "field-id": 515},
        {"name": "min_sequence_number", "type": "long", "field-id": 516},
        {"name": "added_snapshot_id", "type": "long", "field-id": 503},
        {"name": "added_files_count", "type": "int", "field-id": 504},
        {"name": "existing_files_count", "type": "int", "field-id": 505},
        {"name": "deleted_files_count", "type": "int", "field-id": 506},
        {"name": "added_rows_count", "type": "long", "field-id": 512},
        {"name": "existing_rows_count", "type": "long", "field-id": 513},
        {"name": "deleted_rows_count", "type": "long", "field-id": 514},
        _optional(
            "partitions",
            {
                "type": "array",
                "element-id": 508,
                "items": {
                    "type": "record",
                    "name": "r508",
                    "fields": [
                        {"name": "contains_null", "type": "boolean", "field-id": 509},
                        _optional("contains_nan", "boolean", 518),
                        _optional("lower_bound", "bytes", 510),
                        _optional("upper_bound", "bytes", 511),
                    ],
                },
            },
            507,
        ),
        _optional("key_metadata", "bytes", 519),
    ],
}

# The manifest-list columns carried over verbatim from the parent snapshot's
# list; `partitions` is the one nested column.
MANIFEST_FILE_COLUMNS = [field["name"] for field in MANIFEST_FILE_SCHEMA["fields"]]


def new_snapshot_id() -> int:
    """A positive 63-bit id from a random UUID (pyiceberg's own recipe, kept
    non-negative)."""
    return uuid.uuid4().int & ((1 << 63) - 1)


def now_ms() -> int:
    return int(time.time() * 1000)


def table_schema_json(table) -> str:
    return table.schema().model_dump_json()


def partition_spec_json(table) -> str:
    return json.dumps(json.loads(table.spec().model_dump_json())["fields"])


# ── refusals ──────────────────────────────────────────────────────────────────

# Table properties that move or reshape data files; this writer honours none of
# them, so a table that sets one is refused rather than written somewhere else.
_REFUSED_PROPERTIES = ("write.data.path", "write.object-storage.enabled", "write.object-storage.path")


def refuse_unwritable(table) -> None:
    name = ".".join(table.name())
    metadata = table.metadata
    if metadata.format_version != FORMAT_VERSION:
        raise NotImplementedError(
            f"opteryx-iceberg writes format-version {FORMAT_VERSION} tables only; "
            f"{name} is format-version {metadata.format_version}."
        )
    if not table.spec().is_unpartitioned():
        raise NotImplementedError(f"opteryx-iceberg does not write partitioned tables ({name}).")
    present = [key for key in _REFUSED_PROPERTIES if key in metadata.properties]
    file_format = metadata.properties.get("write.format.default", "parquet").lower()
    if present or file_format != "parquet":
        raise NotImplementedError(
            f"opteryx-iceberg writes Parquet data files at the default location only; {name} "
            f"sets {present or ['write.format.default=' + file_format]}."
        )
    nested = [f.name for f in table.schema().fields if not f.field_type.is_primitive]
    if nested:
        raise NotImplementedError(f"opteryx-iceberg does not write nested columns ({name}: {nested}).")


def _location(table) -> str:
    return table.metadata.location.rstrip("/")


# ── data files ────────────────────────────────────────────────────────────────


def open_data_file_writer(
    table,
    io,
    sorted_by=None,
    sorted_descending: bool = False,
    write_options: dict | None = None,
    statistics: bool = True,
):
    from opteryx_catalog.catalog.dataset import open_data_file_writer_at
    from opteryx_catalog.iops.fileio import WRITE_PARQUET_OPTIONS

    from opteryx_iceberg.fileio import IcebergFileIO

    refuse_unwritable(table)
    field_id_by_name = {field.name: field.field_id for field in table.schema().fields}
    options = dict(WRITE_PARQUET_OPTIONS if write_options is None else write_options)
    options[PARQUET_FIELD_IDS_OPTION] = field_id_by_name
    return open_data_file_writer_at(
        IcebergFileIO(io.properties, io.policy),
        _location(table),
        field_id_by_name,
        sorted_by=sorted_by,
        sorted_descending=sorted_descending,
        write_options=options,
        statistics=statistics,
    )


# ── the append commit ─────────────────────────────────────────────────────────


def _physical_by_field_id(table) -> dict:
    from opteryx_iceberg.dataset import _display_type

    from opteryx.types.logical_type import parse_column_type

    return {
        field.field_id: parse_column_type(_display_type(field.field_type)).physical
        for field in table.schema().fields
    }


def _parent_manifest_files(table, parent, io) -> dict:
    """The parent snapshot's manifest-list rows, column by column, to carry over."""
    from rugo.avro import read_avro

    from opteryx_iceberg.fileio import _ObjectReader

    columns = {name: [] for name in MANIFEST_FILE_COLUMNS}
    if parent is None:
        return columns
    data = _ObjectReader(io.properties, io.policy).read(parent.manifest_list)
    with read_avro(data, columns=MANIFEST_FILE_COLUMNS, reader_schema=MANIFEST_FILE_SCHEMA, max_threads=1) as reader:
        for morsel in reader:
            for name in MANIFEST_FILE_COLUMNS:
                columns[name].extend(morsel.column(name).to_pylist())
    return columns


def _summary(parent, files: int, records: int, size: int):
    from pyiceberg.table.snapshots import Operation
    from pyiceberg.table.snapshots import Summary

    counters = {"added-data-files": files, "added-records": records, "added-files-size": size}
    previous = parent.summary if parent is not None else None
    for total, added in (
        ("total-data-files", files),
        ("total-records", records),
        ("total-files-size", size),
    ):
        if parent is None:
            counters[total] = added
        elif previous is not None and total in previous.additional_properties:
            counters[total] = int(previous.additional_properties[total]) + added
    return Summary(Operation.APPEND, **{key: str(value) for key, value in counters.items()})


def append_files(table, io, manifest: bytes):
    """Commit the engine's written files as one fast-append snapshot; returns
    the table as the catalog holds it after the commit. `io` is the dataset's
    FileIO: its properties and its location policy govern every write."""
    from opteryx_catalog.catalog.manifest import read_manifest_rows
    from opteryx_catalog.exceptions import SnapshotRaceError
    from pyiceberg.exceptions import CommitFailedException
    from pyiceberg.table.refs import SnapshotRefType
    from pyiceberg.table.snapshots import Snapshot
    from pyiceberg.table.update import AddSnapshotUpdate
    from pyiceberg.table.update import AssertRefSnapshotId
    from pyiceberg.table.update import SetSnapshotRefUpdate

    from opteryx_iceberg.fileio import _ObjectWriter

    refuse_unwritable(table)
    rows = read_manifest_rows(manifest)
    location = _location(table)
    for row in rows:
        if not row["file_path"].startswith(f"{location}/data/"):
            raise ValueError(
                f"Iceberg INSERT: refusing to register {row['file_path']!r} - it is not under "
                f"{location}/data/."
            )

    metadata = table.metadata
    parent = metadata.current_snapshot()
    snapshot_id = new_snapshot_id()
    sequence_number = metadata.last_sequence_number + 1
    physical = _physical_by_field_id(table)
    writer = _ObjectWriter(io.properties, io.policy)
    token = uuid.uuid4()

    manifest_path = f"{location}/metadata/{token}-m0.avro"
    manifest_bytes = write_avro_file(
        MANIFEST_ENTRY_SCHEMA,
        {
            "schema": table_schema_json(table),
            "schema-id": str(table.schema().schema_id),
            "partition-spec": partition_spec_json(table),
            "partition-spec-id": str(table.spec().spec_id),
            "format-version": str(FORMAT_VERSION),
            "content": "data",
        },
        {
            "status": (DrakenType.INT32, [1] * len(rows)),
            "snapshot_id": (DrakenType.INT64, [snapshot_id] * len(rows)),
            "sequence_number": (DrakenType.INT64, [None] * len(rows)),
            "file_sequence_number": (DrakenType.INT64, [None] * len(rows)),
            "data_file": (None, [json.dumps(data_file_json(row, physical)) for row in rows]),
        },
    )

    carried = _parent_manifest_files(table, parent, io)
    added = {
        "manifest_path": manifest_path,
        "manifest_length": len(manifest_bytes),
        "partition_spec_id": table.spec().spec_id,
        "content": 0,
        "sequence_number": sequence_number,
        "min_sequence_number": sequence_number,
        "added_snapshot_id": snapshot_id,
        "added_files_count": len(rows),
        "existing_files_count": 0,
        "deleted_files_count": 0,
        "added_rows_count": sum(row["record_count"] for row in rows),
        "existing_rows_count": 0,
        "deleted_rows_count": 0,
        "partitions": "[]",
        "key_metadata": None,
    }
    list_columns = {name: [added[name], *carried[name]] for name in MANIFEST_FILE_COLUMNS}
    list_path = f"{location}/metadata/snap-{snapshot_id}-1-{token}.avro"
    list_bytes = write_avro_file(
        MANIFEST_FILE_SCHEMA,
        {
            "snapshot-id": str(snapshot_id),
            "parent-snapshot-id": "null" if parent is None else str(parent.snapshot_id),
            "sequence-number": str(sequence_number),
            "format-version": str(FORMAT_VERSION),
        },
        {
            name: (_MANIFEST_FILE_TYPES[name], values)
            for name, values in list_columns.items()
        },
    )

    written = []
    try:
        for path, data in ((manifest_path, manifest_bytes), (list_path, list_bytes)):
            stream = writer.open(path)
            stream.write(data)
            stream.close()
            written.append(path)
        snapshot = Snapshot(
            snapshot_id=snapshot_id,
            parent_snapshot_id=None if parent is None else parent.snapshot_id,
            sequence_number=sequence_number,
            timestamp_ms=now_ms(),
            manifest_list=list_path,
            summary=_summary(
                parent,
                len(rows),
                added["added_rows_count"],
                sum(row["file_size_in_bytes"] for row in rows),
            ),
            schema_id=table.schema().schema_id,
        )
        table.catalog.commit_table(
            table,
            (AssertRefSnapshotId(ref="main", snapshot_id=None if parent is None else parent.snapshot_id),),
            (
                AddSnapshotUpdate(snapshot=snapshot),
                SetSnapshotRefUpdate(ref_name="main", type=SnapshotRefType.BRANCH, snapshot_id=snapshot_id),
            ),
        )
    except CommitFailedException as exc:
        for path in written:
            writer.delete(path)
        raise SnapshotRaceError(
            f"Iceberg table {'.'.join(table.name())} moved on during this INSERT: {exc}"
        ) from exc
    except BaseException:
        for path in written:
            writer.delete(path)
        raise
    return table.catalog.load_table(table.name())


# Manifest-list column -> DrakenType of its plain column; None = JSON text
# encoded against the schema field (the nested `partitions`).
_MANIFEST_FILE_TYPES = {
    "manifest_path": DrakenType.VARCHAR,
    "manifest_length": DrakenType.INT64,
    "partition_spec_id": DrakenType.INT32,
    "content": DrakenType.INT32,
    "sequence_number": DrakenType.INT64,
    "min_sequence_number": DrakenType.INT64,
    "added_snapshot_id": DrakenType.INT64,
    "added_files_count": DrakenType.INT32,
    "existing_files_count": DrakenType.INT32,
    "deleted_files_count": DrakenType.INT32,
    "added_rows_count": DrakenType.INT64,
    "existing_rows_count": DrakenType.INT64,
    "deleted_rows_count": DrakenType.INT64,
    "partitions": None,
    "key_metadata": DrakenType.VARBINARY,
}


def write_avro_file(schema: dict, metadata: dict, columns: dict) -> bytes:
    """One Avro container, written by rugo to `schema` with header `metadata`.
    `columns` maps a field name to (DrakenType, values) for a plain column, or
    (None, JSON texts) for a nested one (bytes in base64)."""
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.avro import write_avro

    names = list(columns)
    vectors = [
        vector_from_sequence(values, DrakenType.VARCHAR if dtype is None else dtype)
        for dtype, values in columns.values()
    ]
    return write_avro(
        Morsel.from_vectors(names, vectors),
        schema=schema,
        metadata=metadata,
        json_columns=[name for name, (dtype, _) in columns.items() if dtype is None],
        json_bytes="base64",
    )
