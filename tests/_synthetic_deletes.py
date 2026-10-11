"""Merge-on-read snapshots for tests.

pyiceberg has no merge-on-read write path, so these write a DELETES manifest with
pyiceberg's own manifest writer and a manifest list that adds it to the table's
current data manifests - the shape Spark/Flink/Trino commit. `commit` makes the
snapshot the table's current one through the catalog.
"""

import time
import uuid

from pyiceberg.manifest import DataFile
from pyiceberg.manifest import DataFileContent
from pyiceberg.manifest import FileFormat
from pyiceberg.manifest import ManifestContent
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

# Iceberg's reserved field id for a position delete file's `file_path` column.
PATH_FIELD_ID = 2147483546


class _DeleteManifestWriter(ManifestWriterV2):
    def content(self):
        return ManifestContent.DELETES

    @property
    def _meta(self):
        return {**super()._meta, "content": "deletes"}


def delete_file(
    path,
    partition,
    content=DataFileContent.POSITION_DELETES,
    record_count=1,
    path_bounds=None,
):
    """A delete file entry. `partition` is the tuple of partition values;
    `path_bounds` (lower, upper) are the file_path column's bounds."""
    args = {
        "content": content,
        "file_path": path,
        "file_format": FileFormat.PARQUET,
        "partition": Record(*partition),
        "record_count": record_count,
        "file_size_in_bytes": 100,
    }
    if path_bounds is not None:
        args["lower_bounds"] = {PATH_FIELD_ID: path_bounds[0].encode()}
        args["upper_bounds"] = {PATH_FIELD_ID: path_bounds[1].encode()}
    if content == DataFileContent.EQUALITY_DELETES:
        args["equality_ids"] = [1]
    return DataFile.from_args(_table_format_version=2, **args)


def snapshot_with_deletes(table, deletes, manifest_sequence_number=None):
    """A snapshot (not committed) of the table's current data manifests plus one
    DELETES manifest holding `deletes`, a list of (DataFile, entry sequence
    number or None to inherit). `manifest_sequence_number` pins the delete
    manifest's sequence numbers instead of the new snapshot's."""
    meta = table.metadata
    parent = meta.current_snapshot()
    snapshot_id = uuid.uuid4().int >> 66
    sequence_number = meta.last_sequence_number + 1
    folder = f"{meta.location}/metadata"

    writer = _DeleteManifestWriter(
        meta.spec(),
        meta.schema(),
        table.io.new_output(f"{folder}/{uuid.uuid4()}-deletes.avro"),
        snapshot_id,
        "deflate",
    )
    with writer:
        for data_file, entry_sequence_number in deletes:
            writer.add_entry(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=snapshot_id,
                    sequence_number=entry_sequence_number,
                    file_sequence_number=entry_sequence_number,
                    data_file=data_file,
                )
            )
    delete_manifest = writer.to_manifest_file()
    if manifest_sequence_number is not None:
        delete_manifest.sequence_number = manifest_sequence_number
        delete_manifest.min_sequence_number = manifest_sequence_number

    list_path = f"{folder}/snap-{snapshot_id}-{uuid.uuid4()}.avro"
    with write_manifest_list(
        2, table.io.new_output(list_path), snapshot_id, parent.snapshot_id, sequence_number, "deflate"
    ) as list_writer:
        list_writer.add_manifests(parent.manifests(table.io) + [delete_manifest])

    return Snapshot(
        snapshot_id=snapshot_id,
        parent_snapshot_id=parent.snapshot_id,
        sequence_number=sequence_number,
        timestamp_ms=int(time.time() * 1000),
        manifest_list=list_path,
        summary=Summary(Operation.OVERWRITE),
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
