"""`opteryx_iceberg.manifests.plan_data_files` - the snapshot's manifest list and
manifests read with rugo's Avro reader - against pyiceberg's own
`plan_files()`, the oracle it replaces.

Real tables (v1, v2, a v1 table upgraded to v2 so one snapshot mixes v1 and v2
manifests, copy-on-write deletes leaving EXISTING and DELETED entries) are
compared snapshot by snapshot. Merge-on-read is driven by synthetic delete
manifests (tests/_synthetic_deletes.py): pyiceberg's planner decides whether a
delete applies, and the planner here must refuse exactly when it does.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))

import pyarrow as pa
import pytest
from _synthetic_deletes import commit
from _synthetic_deletes import delete_file
from _synthetic_deletes import snapshot_with_deletes
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.conversions import from_bytes
from pyiceberg.expressions import EqualTo
from pyiceberg.manifest import DataFileContent
from pyiceberg.partitioning import PartitionField
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import ManifestGroupPlanner
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DoubleType
from pyiceberg.types import LongType
from pyiceberg.types import NestedField
from pyiceberg.types import StringType

from opteryx_iceberg.dataset import _column_type
from opteryx_iceberg.manifests import plan_data_files

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "cat", StringType(), required=False),
    NestedField(3, "v", DoubleType(), required=False),
)
BY_CAT = PartitionSpec(PartitionField(2, 1000, IdentityTransform(), "cat"))


def _batch(k, cats=("c0", "c1", "c2")):
    n = 30
    return pa.table(
        {
            "id": pa.array([k * 100 + j for j in range(n)], pa.int64()),
            "cat": pa.array([None if j % 11 == 0 else cats[j % len(cats)] for j in range(n)]),
            "v": pa.array([None if j % 4 == 0 else j / 3 for j in range(n)]),
        }
    )


def _catalog(tmp_path):
    catalog = SqlCatalog("t", uri=f"sqlite:///{tmp_path}/c.db", warehouse=f"file://{tmp_path}/wh")
    catalog.create_namespace("n")
    return catalog


def _ordinal_types(table):
    types = {}
    for field in table.schema().fields:
        column_type = _column_type(field.field_type)
        if column_type is not None:
            types[field.field_id] = column_type.physical
    return types


def _plan(table, snapshot):
    return plan_data_files(table.io, snapshot, "n.t", _ordinal_types(table))


def _summary(data_file):
    return (
        data_file.file_path,
        data_file.record_count,
        data_file.file_size_in_bytes,
        data_file.field_ids,
        data_file.min_values,
        data_file.max_values,
        data_file.null_counts,
    )


def _expected(table, data_file):
    """What the Python path built for a pyiceberg DataFile: pyiceberg's
    `from_bytes`, then `ColumnType.ordinalize` - the oracle for the native
    decoding (opteryx-core src/cpp/planner/iceberg_bounds.hpp)."""
    by_id = {f.field_id: f for f in table.schema().fields}
    lower = data_file.lower_bounds or {}
    upper = data_file.upper_bounds or {}
    nulls = data_file.null_value_counts or {}
    ids = sorted(set(lower) | set(upper))

    def ordinal(bounds, field_id):
        field = by_id.get(field_id)
        column_type = None if field is None else _column_type(field.field_type)
        if field_id not in bounds or column_type is None:
            return None
        return column_type.ordinalize(from_bytes(field.field_type, bounds[field_id]))

    return (
        data_file.file_path,
        data_file.record_count,
        data_file.file_size_in_bytes,
        ids,
        [ordinal(lower, f) for f in ids],
        [ordinal(upper, f) for f in ids],
        [nulls.get(f) for f in ids],
    )


def _assert_matches_oracle(table, snapshot):
    oracle = list(table.scan(snapshot_id=snapshot.snapshot_id).plan_files())
    assert not any(task.delete_files for task in oracle)
    got = _plan(table, snapshot)
    assert [_summary(d) for d in got] == [_expected(table, task.file) for task in oracle]
    return got


@pytest.mark.parametrize("version", ["1", "2"])
@pytest.mark.parametrize("spec", [BY_CAT, PartitionSpec()], ids=["by-cat", "unpartitioned"])
def test_every_snapshot_matches_pyiceberg(tmp_path, version, spec):
    table = _catalog(tmp_path).create_table(
        "n.t", schema=SCHEMA, partition_spec=spec, properties={"format-version": version}
    )
    for k in range(4):
        table.append(_batch(k))
    # A copy-on-write delete rewrites files: manifests then carry DELETED and
    # EXISTING entries beside ADDED ones.
    table.delete(EqualTo("cat", "c1"))
    table.append(_batch(9))

    snapshots = table.snapshots()
    assert len(snapshots) >= 6
    for snapshot in snapshots:
        _assert_matches_oracle(table, snapshot)
    # The last snapshot must have live files, or the comparison proves little.
    assert _assert_matches_oracle(table, table.current_snapshot())


def test_v1_table_upgraded_to_v2_mixes_manifest_versions(tmp_path):
    table = _catalog(tmp_path).create_table(
        "n.t", schema=SCHEMA, partition_spec=BY_CAT, properties={"format-version": "1"}
    )
    table.append(_batch(0))
    table.append(_batch(1))
    with table.transaction() as transaction:
        transaction.upgrade_table_version(format_version=2)
    table.append(_batch(2))
    table.delete(EqualTo("cat", "c2"))
    assert table.metadata.format_version == 2

    for snapshot in table.snapshots():
        _assert_matches_oracle(table, snapshot)


# ---------------------------------------------------------------------------
# Merge-on-read: refuse exactly when pyiceberg attaches a delete file.


@pytest.fixture
def partitioned(tmp_path):
    """v2, partitioned by `cat`: two appends (data sequence numbers 1 and 2)."""
    table = _catalog(tmp_path).create_table(
        "n.t", schema=SCHEMA, partition_spec=BY_CAT, properties={"format-version": "2"}
    )
    table.append(_batch(0, cats=("c0", "c1")))
    table.append(_batch(1, cats=("c0", "c1")))
    return table


def _c0_paths(table):
    return sorted(
        task.file.file_path
        for task in table.scan().plan_files()
        if task.file.partition[0] == "c0"
    )


def _decide(table, snapshot):
    """'refused' or 'read' from both planners - they must agree."""
    oracle = ManifestGroupPlanner(table.metadata, table.io).plan_files(
        snapshot.manifests(table.io)
    )
    expected = "refused" if any(task.delete_files for task in oracle) else "read"
    try:
        got = _plan(table, snapshot)
        decided = "read"
    except NotImplementedError as refusal:
        assert "merge-on-read" in str(refusal)
        assert "deleted rows as live data" in str(refusal)
        decided = "refused"
    assert decided == expected
    if decided == "read":
        assert [_summary(d) for d in got] == [_expected(table, task.file) for task in oracle]
    return decided


def test_position_delete_in_the_partition_is_refused(partitioned):
    snap = snapshot_with_deletes(partitioned, [(delete_file("d.parquet", ("c0",)), None)])
    assert _decide(partitioned, snap) == "refused"


def test_position_delete_in_another_partition_reads(partitioned):
    snap = snapshot_with_deletes(partitioned, [(delete_file("d.parquet", ("zz",)), None)])
    assert _decide(partitioned, snap) == "read"


def test_position_delete_older_than_some_data_still_applies_to_older_data(partitioned):
    # Entry sequence number 1: applies to the first append's c0 file (seq 1),
    # not the second's (seq 2).
    snap = snapshot_with_deletes(partitioned, [(delete_file("d.parquet", ("c0",)), 1)])
    assert _decide(partitioned, snap) == "refused"


def test_delete_manifest_older_than_every_data_file_is_not_opened(partitioned):
    # The manifest's sequence number (0) is below every data file's, so it is
    # skipped unopened - even though its entry claims sequence number 2, which
    # would apply to the second append's files had it been read.
    snap = snapshot_with_deletes(
        partitioned, [(delete_file("d.parquet", ("c0",)), 2)], manifest_sequence_number=0
    )
    assert _decide(partitioned, snap) == "read"


def test_inherited_data_sequence_numbers_decide_the_match(tmp_path):
    # pyiceberg writes ADDED entries with a null sequence number, inherited from
    # the manifest list. c0's only file is in the second append (seq 2), so a
    # delete at seq 1 predates it; read without inheritance it would be seq 0
    # and the delete would apply.
    table = _catalog(tmp_path).create_table(
        "n.t", schema=SCHEMA, partition_spec=BY_CAT, properties={"format-version": "2"}
    )
    table.append(_batch(0, cats=("c1",)))
    table.append(_batch(1, cats=("c0",)))
    snap = snapshot_with_deletes(table, [(delete_file("d.parquet", ("c0",)), 1)])
    assert _decide(table, snap) == "read"


def test_position_delete_naming_a_data_file_is_refused(partitioned):
    target = _c0_paths(partitioned)[0]
    snap = snapshot_with_deletes(
        partitioned, [(delete_file("d.parquet", ("zz",), path_bounds=(target, target)), None)]
    )
    assert _decide(partitioned, snap) == "refused"


def test_position_delete_naming_an_absent_file_reads(partitioned):
    gone = "file:///nowhere/gone.parquet"
    snap = snapshot_with_deletes(
        partitioned, [(delete_file("d.parquet", ("c0",), path_bounds=(gone, gone)), None)]
    )
    assert _decide(partitioned, snap) == "read"


def test_position_delete_whose_path_bounds_exclude_the_data_reads(partitioned):
    snap = snapshot_with_deletes(
        partitioned, [(delete_file("d.parquet", ("c0",), path_bounds=("a", "b")), None)]
    )
    assert _decide(partitioned, snap) == "read"


def test_position_delete_whose_path_bounds_cover_the_data_is_refused(partitioned):
    paths = _c0_paths(partitioned)
    snap = snapshot_with_deletes(
        partitioned, [(delete_file("d.parquet", ("c0",), path_bounds=(paths[0], paths[-1] + "~")), None)]
    )
    assert _decide(partitioned, snap) == "refused"


def test_empty_delete_file_is_ignored(partitioned):
    snap = snapshot_with_deletes(
        partitioned, [(delete_file("d.parquet", ("c0",), record_count=0), None)]
    )
    assert _decide(partitioned, snap) == "read"


def test_equality_delete_is_refused(partitioned):
    snap = snapshot_with_deletes(
        partitioned,
        [(delete_file("d.parquet", ("zz",), content=DataFileContent.EQUALITY_DELETES), None)],
    )
    # pyiceberg raises on any live equality delete; so does the planner here.
    with pytest.raises(ValueError, match="equality deletes"):
        ManifestGroupPlanner(partitioned.metadata, partitioned.io).plan_files(
            snap.manifests(partitioned.io)
        )
    with pytest.raises(NotImplementedError, match="equality delete files"):
        _plan(partitioned, snap)


def test_committed_merge_on_read_snapshot_is_current(partitioned):
    """`commit` really makes the synthetic snapshot current - the end-to-end
    refusal test in test_remote_read_surface.py depends on it."""
    snap = snapshot_with_deletes(partitioned, [(delete_file("d.parquet", ("c0",)), None)])
    commit(partitioned, snap)
    assert partitioned.current_snapshot().snapshot_id == snap.snapshot_id
    assert any(task.delete_files for task in partitioned.scan().plan_files())


# ---------------------------------------------------------------------------
# Native bound decoding: every Iceberg type opteryx binds, against the Python path.


def _typed_batch(k):
    import datetime
    import decimal

    sign = -1 if k % 2 else 1
    return pa.table(
        {
            "b": pa.array([k % 2 == 0, None, True], pa.bool_()),
            "i": pa.array([sign * 7, -(2**31), 2**31 - 1 - k], pa.int32()),
            "l": pa.array([sign * 2**40, -(2**63) + k, 5], pa.int64()),
            "f": pa.array([sign * 1.5, -0.25, 3.0e30], pa.float32()),
            "d": pa.array([sign * 2.5e-300, -1.0e300, 0.0], pa.float64()),
            "dt": pa.array(
                [datetime.date(1901, 1, 1 + k), datetime.date(2038, 1, 19), None], pa.date32()
            ),
            # Iceberg `timestamp` (no zone): naive by definition.
            "ts": pa.array(
                [datetime.datetime(1960, 3, 1, 0, 0, k), datetime.datetime(2026, 10, 10, 12), None],  # noqa: DTZ001
                pa.timestamp("us"),
            ),
            "tz": pa.array(
                [datetime.datetime(1999, 12, 31, 23, 59, k, tzinfo=datetime.UTC), None, None],
                pa.timestamp("us", tz="UTC"),
            ),
            "s": pa.array([f"é{k}", "a much longer string than sixteen chars", "Ω"], pa.string()),
            "bin": pa.array([bytes([255, k]), b"\x00", b"\x7f" * 20], pa.binary()),
            "d9": pa.array(
                [decimal.Decimal(sign * 1234567) / 100, decimal.Decimal("-0.01"), None],
                pa.decimal128(9, 2),
            ),
            "d18": pa.array(
                [decimal.Decimal(sign * 99999999999999) / 10000, decimal.Decimal("0.0001"), None],
                pa.decimal128(18, 4),
            ),
            # TimeType: no opteryx ordinal type, so no bound.
            "t": pa.array([1000 * k, 2000, None], pa.time64("us")),
            "allnull": pa.array([None, None, None], pa.int64()),
        }
    )


@pytest.mark.parametrize("version", ["1", "2"])
def test_every_bound_type_matches_the_python_path(tmp_path, version):
    batch = _typed_batch(0)
    table = _catalog(tmp_path).create_table(
        "n.t", schema=batch.schema, properties={"format-version": version}
    )
    for k in range(4):
        table.append(_typed_batch(k))
    got = _assert_matches_oracle(table, table.current_snapshot())
    # The comparison must have exercised a bound of every mapped type.
    by_name = {f.name: f.field_id for f in table.schema().fields}
    bounded = {
        fid for d in got for fid, low in zip(d.field_ids, d.min_values) if low is not None
    }
    mapped = set(by_name) - {"t", "allnull"}
    assert {by_name[n] for n in mapped} <= bounded
    assert by_name["t"] not in bounded


def test_a_decimal128_column_has_no_bounds_and_the_others_keep_theirs(tmp_path):
    """Draken has no ordinal key for DECIMAL128 (precision > 18), so its bounds
    are skipped - the scan still plans, and the other columns keep theirs."""
    import decimal

    batch = pa.table(
        {
            "w": pa.array([decimal.Decimal("1.25"), decimal.Decimal("-9.50")], pa.decimal128(30, 2)),
            "n": pa.array([3, 4], pa.int64()),
        }
    )
    table = _catalog(tmp_path).create_table("n.t", schema=batch.schema)
    table.append(batch)
    by_name = {f.name: f.field_id for f in table.schema().fields}
    (got,) = _assert_matches_oracle(table, table.current_snapshot())
    bounds = dict(zip(got.field_ids, zip(got.min_values, got.max_values)))
    # Iceberg recorded a bound for `w`; it is left out, not refused.
    assert bounds[by_name["w"]] == (None, None)
    assert bounds[by_name["n"]] == (3, 4)


def test_a_data_file_carries_the_format_its_manifest_entry_declares(tmp_path):
    table = _catalog(tmp_path).create_table("n.t", schema=SCHEMA)
    table.append(_batch(0))
    got = _plan(table, table.current_snapshot())
    assert [d.file_format for d in got] == ["parquet"]


@pytest.mark.parametrize("declared", ["PARQUET", "parquet", "AVRO", "Avro", "ORC", "orc"])
def test_a_spec_format_is_stamped_lower_case(declared):
    from opteryx_iceberg.manifests import _data_file_format

    assert _data_file_format("n.t", "f", declared) == declared.lower()


@pytest.mark.parametrize("declared", ["PUFFIN", "csv", "", None])
def test_a_format_the_spec_does_not_allow_is_refused_by_name(declared):
    from opteryx_iceberg.manifests import _data_file_format

    with pytest.raises(ValueError, match="PARQUET, AVRO and ORC"):
        _data_file_format("n.t", "f", declared)
