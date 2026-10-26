"""Iceberg tables whose data files are Avro (or any other declared format), for tests.

pyiceberg writes Parquet only, so these write the Avro data file with fastavro, in the
shape Iceberg's Avro writer produces (every field carries a `field-id`, optional fields
are `["null", T]`), then a DATA manifest listing it with pyiceberg's own manifest writer
and a manifest list adding it to the table's current manifests. `commit` makes the
snapshot the table's current one through the catalog.
"""

import itertools
import time
import uuid

import fastavro
from pyiceberg.manifest import DataFile
from pyiceberg.manifest import DataFileContent
from pyiceberg.manifest import FileFormat
from pyiceberg.manifest import ManifestEntry
from pyiceberg.manifest import ManifestEntryStatus
from pyiceberg.manifest import ManifestWriterV2
from pyiceberg.manifest import write_manifest_list
from pyiceberg.table.snapshots import Operation
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.snapshots import Summary
from pyiceberg.table.update import AddSnapshotUpdate
from pyiceberg.table.update import SetSnapshotRefUpdate
from pyiceberg.typedef import Record

_unique = itertools.count()


def avro_field(name, field_id, avro_type, nullable=True):
    """One field of an Iceberg-written Avro record."""
    if nullable:
        return {"name": name, "type": ["null", avro_type], "default": None, "field-id": field_id}
    return {"name": name, "type": avro_type, "field-id": field_id}


def write_avro_data_file(path, fields, records, codec="deflate"):
    """An Avro data file at `path` (a local path); returns its size in bytes."""
    schema = {"type": "record", "name": "table", "fields": fields}
    with open(path, "wb") as f:
        fastavro.writer(f, schema, records, codec=codec)
    with open(path, "rb") as f:
        return len(f.read())


def data_file(path, file_format, record_count, size):
    """A DATA entry for `path` (a local path, or a URI), declared as `file_format` (a
    pyiceberg FileFormat)."""
    location = str(path)
    return DataFile.from_args(
        _table_format_version=2,
        content=DataFileContent.DATA,
        file_path=location if "://" in location else f"file://{location}",
        file_format=file_format,
        partition=Record(),
        record_count=record_count,
        file_size_in_bytes=size,
    )


def snapshot_with_data_files(table, data_files):
    """A snapshot (not committed) of the table's current manifests plus one DATA
    manifest holding `data_files`."""
    meta = table.metadata
    parent = meta.current_snapshot()
    snapshot_id = uuid.uuid4().int >> 66
    sequence_number = meta.last_sequence_number + 1
    folder = f"{meta.location}/metadata"
    n = next(_unique)

    writer = ManifestWriterV2(
        meta.spec(),
        meta.schema(),
        table.io.new_output(f"{folder}/{uuid.uuid4()}-data-{n}.avro"),
        snapshot_id,
        "deflate",
    )
    with writer:
        for entry in data_files:
            writer.add_entry(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=snapshot_id,
                    sequence_number=None,
                    file_sequence_number=None,
                    data_file=entry,
                )
            )
    data_manifest = writer.to_manifest_file()

    list_path = f"{folder}/snap-{snapshot_id}-{uuid.uuid4()}.avro"
    with write_manifest_list(
        2,
        table.io.new_output(list_path),
        snapshot_id,
        None if parent is None else parent.snapshot_id,
        sequence_number,
        "deflate",
    ) as list_writer:
        existing = [] if parent is None else parent.manifests(table.io)
        list_writer.add_manifests(existing + [data_manifest])

    return Snapshot(
        snapshot_id=snapshot_id,
        parent_snapshot_id=None if parent is None else parent.snapshot_id,
        sequence_number=sequence_number,
        timestamp_ms=int(time.time() * 1000),
        manifest_list=list_path,
        summary=Summary(Operation.APPEND),
        schema_id=meta.current_schema_id,
    )


def commit(table, snapshot):
    """Make `snapshot` the table's current snapshot."""
    table._do_commit(
        updates=(
            AddSnapshotUpdate(snapshot=snapshot),
            SetSnapshotRefUpdate(
                ref_name="main", type="branch", snapshot_id=snapshot.snapshot_id
            ),
        ),
        requirements=(),
    )


def add_files(table, data_files):
    commit(table, snapshot_with_data_files(table, data_files))


__all__ = [
    "FileFormat",
    "add_files",
    "avro_field",
    "data_file",
    "write_avro_data_file",
]
