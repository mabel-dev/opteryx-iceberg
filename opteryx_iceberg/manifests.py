"""A snapshot's live data files, read from Iceberg's manifest list and manifests
with rugo's Avro reader - in place of pyiceberg's `table.scan().plan_files()`.

pyiceberg is still the catalog client (metadata.json, snapshots, schemas); only
the Avro side of planning moves here (opteryx-core docs/AVRO_READER_DESIGN.md §9,
D4 "Python in the backend for starters"). The decisions are pyiceberg's local
`plan_files()` with no row filter, made the same way:

- manifest list -> manifests. A DELETES manifest whose sequence number is below
  the smallest data sequence number is not opened (its deletes predate the data).
- entries with status DELETED are dropped, as are files with record_count 0
  (pyiceberg's metrics evaluator excludes empty files by default).
- sequence numbers are inherited from the manifest list entry when null and the
  entry is ADDED (or the manifest's own sequence number is 0, v1).
- position deletes are matched to data files by exact referenced path, or by
  partition (spec id + partition value) with sequence number >= the data file's
  and the delete's file_path bounds covering the data file's path. A data file
  with any matching delete is refused (merge-on-read).
- any live equality delete is refused.

A data file's bounds come out as the opteryx manifest's lists - field ids,
ordinal min/max keys, null counts - decoded natively
(`opteryx.compiled.planner.native_manifest.iceberg_bounds`). A position delete
file's `file_path` bounds stay bytes, read in Python: only delete manifests
carry them, and they are only compared.

v1 and v2 manifests are read through one reader schema each: the file's own
schema, with the v2-only fields added as defaults where the file lacks them
(`content` 0 = DATA, sequence numbers 0 in the list, null in the entry) -
what the Iceberg spec says those fields are in v1.

Manifest lists are always planned here, locally. A REST catalog configured for
server-side scan planning is not consulted for a plan.
"""

from __future__ import annotations

import base64
import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from opteryx.compiled.planner.native_manifest import iceberg_bounds
from rugo.avro import read_avro
from rugo.avro import read_metadata

# Iceberg's reserved field id for a position delete file's `file_path` column.
_PATH_FIELD_ID = 2147483546

_STATUS_DELETED = 2
_STATUS_ADDED = 1
_CONTENT_DATA = 0
_CONTENT_POSITION_DELETES = 1
_CONTENT_EQUALITY_DELETES = 2
_MANIFEST_CONTENT_DELETES = 1

_LIST_COLUMNS = [
    "manifest_path",
    "partition_spec_id",
    "content",
    "sequence_number",
    "min_sequence_number",
]
# v2 manifest-list fields a v1 list does not carry, by field id.
_LIST_DEFAULTS = [
    {"name": "content", "type": "int", "default": 0, "field-id": 517},
    {"name": "sequence_number", "type": "long", "default": 0, "field-id": 515},
    {"name": "min_sequence_number", "type": "long", "default": 0, "field-id": 516},
]

_ENTRY_COLUMNS = [
    "status",
    "sequence_number",
    "data_file.content",
    "data_file.file_path",
    "data_file.record_count",
    "data_file.file_size_in_bytes",
]
_LOWER = "data_file.lower_bounds"
_UPPER = "data_file.upper_bounds"
_NULLS = "data_file.null_value_counts"
_PARTITION_COLUMN = "data_file.partition"
_ENTRY_DEFAULTS = [
    {"name": "sequence_number", "type": ["null", "long"], "default": None, "field-id": 3},
]
_DATA_FILE_DEFAULTS = [
    {"name": "content", "type": "int", "default": 0, "field-id": 134},
]


class ManifestDataFile:
    """One live data file of a snapshot - the fields the opteryx manifest needs.

    `field_ids` is the sorted union of the field ids with a lower or upper bound;
    `min_values` / `max_values` are their ordinal keys and `null_counts` their
    null counts, aligned with `field_ids`, None where absent."""

    __slots__ = (
        "field_ids",
        "file_path",
        "file_size_in_bytes",
        "max_values",
        "min_values",
        "null_counts",
        "record_count",
    )

    def __init__(self, file_path, record_count, file_size_in_bytes, bounds):
        self.file_path = file_path
        self.record_count = record_count
        self.file_size_in_bytes = file_size_in_bytes
        self.field_ids, self.min_values, self.max_values, self.null_counts = bounds


def _with_defaults(fields: list, defaults: list) -> None:
    """Append each default field the record does not already carry (by field id)."""
    present = {f.get("field-id") for f in fields}
    for field in defaults:
        if field["field-id"] not in present:
            fields.append(dict(field))


def _reader_schema(data: bytes) -> dict:
    """The file's own schema with the v2-only entry fields defaulted."""
    schema = read_metadata(data).schema
    _with_defaults(schema["fields"], _ENTRY_DEFAULTS)
    data_file = next(f for f in schema["fields"] if f["name"] == "data_file")["type"]
    _with_defaults(data_file["fields"], _DATA_FILE_DEFAULTS)
    return schema


def _kv(text: str | None) -> dict | None:
    """An Iceberg map<int, binary> - on disk array<record{key, value}>, which the
    reader renders as JSON text, values base64 - as a dict."""
    if text is None:
        return None
    return {e["key"]: base64.b64decode(e["value"]) for e in json.loads(text)}


class _Manifest:
    __slots__ = ("content", "min_sequence_number", "path", "sequence_number", "spec_id")

    def __init__(self, path, spec_id, content, sequence_number, min_sequence_number):
        self.path = path
        self.spec_id = spec_id
        self.content = content
        self.sequence_number = sequence_number
        self.min_sequence_number = min_sequence_number


def _manifest_list(data: bytes) -> list[_Manifest]:
    schema = read_metadata(data).schema
    _with_defaults(schema["fields"], _LIST_DEFAULTS)
    cols = {name: [] for name in _LIST_COLUMNS}
    with read_avro(data, columns=_LIST_COLUMNS, reader_schema=schema, max_threads=1) as reader:
        for morsel in reader:
            for name in _LIST_COLUMNS:
                cols[name].extend(morsel.column(name).to_pylist())
    return [
        _Manifest(*row)
        for row in zip(
            cols["manifest_path"],
            cols["partition_spec_id"],
            cols["content"],
            cols["sequence_number"],
            cols["min_sequence_number"],
        )
    ]


def _data_entries(data: bytes, with_partition: bool, ordinal_types: dict) -> dict:
    """A data manifest's entries; `bounds` holds one (field_ids, min_values,
    max_values, null_counts) per entry, decoded natively."""
    scalars = _ENTRY_COLUMNS + [_PARTITION_COLUMN] if with_partition else _ENTRY_COLUMNS
    cols = {name: [] for name in scalars}
    cols["bounds"] = []
    with read_avro(
        data, columns=scalars + [_LOWER, _UPPER, _NULLS], reader_schema=_reader_schema(data),
        max_threads=1,
    ) as reader:
        for morsel in reader:
            for name in scalars:
                cols[name].extend(morsel.column(name).to_pylist())
            cols["bounds"].extend(
                iceberg_bounds(
                    morsel.num_rows,
                    morsel.column(_LOWER),
                    morsel.column(_UPPER),
                    morsel.column(_NULLS),
                    ordinal_types,
                )
            )
    return cols


def _delete_entries(data: bytes) -> dict:
    """A delete manifest's entries, with partitions and raw lower/upper bounds."""
    columns = [*_ENTRY_COLUMNS, _PARTITION_COLUMN, _LOWER, _UPPER]
    cols = {name: [] for name in columns}
    with read_avro(data, columns=columns, reader_schema=_reader_schema(data), max_threads=1) as reader:
        for morsel in reader:
            for name in columns:
                cols[name].extend(morsel.column(name).to_pylist())
    cols[_LOWER] = [_kv(text) for text in cols[_LOWER]]
    cols[_UPPER] = [_kv(text) for text in cols[_UPPER]]
    return cols


class _Entry:
    """A live entry: a data file (`data_file`) or a position delete file
    (`path_bounds`, its file_path column's (lower, upper) bounds or None)."""

    __slots__ = ("data_file", "partition", "path_bounds", "sequence_number", "spec_id")

    def __init__(self, sequence_number, spec_id, partition, data_file, path_bounds):
        self.sequence_number = sequence_number
        self.spec_id = spec_id
        self.partition = partition
        self.data_file = data_file
        self.path_bounds = path_bounds


def _path_bounds(lower: dict | None, upper: dict | None) -> tuple[bytes, bytes] | None:
    if not lower or not upper or _PATH_FIELD_ID not in lower or _PATH_FIELD_ID not in upper:
        return None
    return lower[_PATH_FIELD_ID], upper[_PATH_FIELD_ID]


class _DeleteIndex:
    """Position delete files by referenced path and by (spec id, partition)."""

    def __init__(self) -> None:
        self._by_partition: dict[tuple, list[_Entry]] = {}
        self._by_path: dict[bytes, list[_Entry]] = {}

    def add(self, entry: _Entry) -> None:
        # The one data file a delete names (UTF-8), when its path bounds agree.
        bounds = entry.path_bounds
        if bounds is not None and bounds[0] and bounds[0] == bounds[1]:
            self._by_path.setdefault(bounds[0], []).append(entry)
        else:
            self._by_partition.setdefault((entry.spec_id, entry.partition), []).append(entry)

    def count_for(self, entry: _Entry) -> int:
        """How many delete files apply to the data file `entry`."""
        path = entry.data_file.file_path.encode("utf-8")
        found = 0
        for delete in self._by_partition.get((entry.spec_id, entry.partition), ()):
            if delete.sequence_number < entry.sequence_number:
                continue
            bounds = delete.path_bounds
            if bounds is None or bounds[0] <= path <= bounds[1]:
                found += 1
        for delete in self._by_path.get(path, ()):
            if delete.sequence_number >= entry.sequence_number:
                found += 1
        return found


def _refuse_equality_deletes(identifier: str) -> None:
    raise NotImplementedError(
        f"opteryx-iceberg (Tier 1, read-only) cannot read {identifier!r}: it "
        "has merge-on-read equality delete files, and reading it without "
        "applying them would return deleted rows as live data. Only "
        "copy-on-write tables are supported."
    )


def plan_data_files(
    io: Any, snapshot: Any, identifier: str, ordinal_types: dict
) -> list[ManifestDataFile]:
    """The live data files of `snapshot` (a pyiceberg Snapshot), read through `io`
    (the table's pyiceberg FileIO). `ordinal_types` maps a field id to the
    DrakenType its column is bound to; bounds of other field ids are None.
    Raises NotImplementedError when delete files apply - see the module
    docstring."""

    def fetch(location: str) -> bytes:
        with io.new_input(location).open() as stream:
            return stream.read()

    manifests = _manifest_list(fetch(snapshot.manifest_list))
    data_seqs = [m.min_sequence_number for m in manifests if m.content != _MANIFEST_CONTENT_DELETES]
    min_sequence_number = min(data_seqs) if data_seqs else 0
    manifests = [
        m
        for m in manifests
        if m.content != _MANIFEST_CONTENT_DELETES or m.sequence_number >= min_sequence_number
    ]
    # Partitions are read only to match position deletes to data files, so only
    # when a delete manifest is open.
    with_partition = any(m.content == _MANIFEST_CONTENT_DELETES for m in manifests)

    def load(manifest: _Manifest) -> dict:
        data = fetch(manifest.path)
        if manifest.content == _MANIFEST_CONTENT_DELETES:
            return _delete_entries(data)
        return _data_entries(data, with_partition, ordinal_types)

    with ThreadPoolExecutor() as pool:
        decoded = list(pool.map(load, manifests))

    data_entries: list[_Entry] = []
    deletes = _DeleteIndex()
    for manifest, cols in zip(manifests, decoded):
        is_delete_manifest = manifest.content == _MANIFEST_CONTENT_DELETES
        partitions = cols[_PARTITION_COLUMN] if with_partition else None
        for i, status in enumerate(cols["status"]):
            if status == _STATUS_DELETED:
                continue
            record_count = cols["data_file.record_count"][i]
            if record_count == 0:
                continue
            sequence_number = cols["sequence_number"][i]
            if sequence_number is None and (
                manifest.sequence_number == 0 or status == _STATUS_ADDED
            ):
                sequence_number = manifest.sequence_number
            partition = None if partitions is None else partitions[i]
            content = cols["data_file.content"][i]
            if content == _CONTENT_DATA and not is_delete_manifest:
                data_file = ManifestDataFile(
                    cols["data_file.file_path"][i],
                    record_count,
                    cols["data_file.file_size_in_bytes"][i],
                    cols["bounds"][i],
                )
                data_entries.append(
                    _Entry(sequence_number or 0, manifest.spec_id, partition, data_file, None)
                )
            elif content == _CONTENT_POSITION_DELETES and is_delete_manifest:
                path_bounds = _path_bounds(cols[_LOWER][i], cols[_UPPER][i])
                deletes.add(
                    _Entry(sequence_number or 0, manifest.spec_id, partition, None, path_bounds)
                )
            elif content == _CONTENT_EQUALITY_DELETES and is_delete_manifest:
                _refuse_equality_deletes(identifier)
            else:
                raise ValueError(
                    f"opteryx-iceberg cannot read {identifier!r}: "
                    f"{'delete' if is_delete_manifest else 'data'} manifest "
                    f"{manifest.path!r} holds a file of content type {content}."
                )

    for entry in data_entries:
        count = deletes.count_for(entry)
        if count:
            raise NotImplementedError(
                f"opteryx-iceberg (Tier 1, read-only) cannot read {identifier!r}: it "
                f"has merge-on-read delete files ({count} on one data file), and "
                "reading it without applying them would return deleted rows as live "
                "data. Only copy-on-write tables are supported."
            )
    return [entry.data_file for entry in data_entries]
