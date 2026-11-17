"""Tier 2 writes - CREATE SCHEMA, CREATE TABLE, INSERT INTO ... SELECT - over a
local SqlCatalog, checked from both sides: opteryx reads back what it wrote,
and plain pyiceberg (an independent Iceberg reader) sees the same snapshots,
manifests, bounds and rows.

Every file is rugo's: Parquet data files with the Iceberg field-ids in their
schema, Avro manifests and manifest lists. Expected values are hand-computed
from the INSERT statements below.
"""

import struct

import pytest
from pyiceberg.catalog.sql import SqlCatalog

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

from opteryx_iceberg import IcebergMetastore

WORKSPACE = "ice_writes"

FIRST = (
    f"INSERT INTO {WORKSPACE}.ns.t SELECT * FROM (VALUES "
    "(1, 'a', CAST('2024-01-01 00:00:00' AS TIMESTAMP), CAST('2024-01-01' AS DATE), 1.5), "
    "(2, 'b', CAST('2024-02-01 00:00:00' AS TIMESTAMP), CAST('2024-02-01' AS DATE), -2.5)"
    ") AS v(id, name, ts, d, x)"
)
SECOND = (
    f"INSERT INTO {WORKSPACE}.ns.t SELECT * FROM (VALUES "
    "(3, CAST(NULL AS VARCHAR), CAST(NULL AS TIMESTAMP), CAST(NULL AS DATE), CAST(NULL AS FLOAT64))"
    ") AS v(id, name, ts, d, x)"
)


def run(session, sql):
    return list(session.execute_to_morsels(sql))


def rows(session, sql):
    out = []
    for morsel in run(session, sql):
        columns = [morsel.column(name).to_pylist() for name in morsel.column_names]
        out.extend(zip(*columns))
    return out


@pytest.fixture(scope="module")
def setup(tmp_path_factory):
    root = tmp_path_factory.mktemp("iceberg-writes")
    (root / "warehouse").mkdir()
    properties = {"uri": f"sqlite:///{root / 'catalog.db'}", "warehouse": f"file://{root / 'warehouse'}"}
    register_workspace(WORKSPACE, OpteryxConnector, catalog=IcebergMetastore, catalog_type="sql", **properties)
    session = opteryx.session(user="tests")
    run(session, f"CREATE SCHEMA {WORKSPACE}.ns")
    run(session, f"CREATE TABLE {WORKSPACE}.ns.t (id BIGINT, name VARCHAR, ts TIMESTAMP, d DATE, x DOUBLE)")
    run(session, FIRST)
    run(session, SECOND)
    return session, SqlCatalog(WORKSPACE, **properties).load_table("ns.t")


def test_opteryx_reads_back(setup):
    session, _ = setup
    assert rows(session, f"SELECT id, name, x FROM {WORKSPACE}.ns.t ORDER BY id") == [
        (1, "a", 1.5),
        (2, "b", -2.5),
        (3, None, None),
    ]


def test_created_table_shape(setup):
    _, table = setup
    assert table.metadata.format_version == 2
    assert table.spec().is_unpartitioned()
    assert [(f.field_id, f.name, str(f.field_type)) for f in table.schema().fields] == [
        (1, "id", "long"),
        (2, "name", "string"),
        (3, "ts", "timestamp"),
        (4, "d", "date"),
        (5, "x", "double"),
    ]


def test_snapshots_are_fast_appends(setup):
    _, table = setup
    snapshots = table.snapshots()
    assert [s.sequence_number for s in snapshots] == [1, 2]
    assert snapshots[1].parent_snapshot_id == snapshots[0].snapshot_id
    assert [s.summary.operation.value for s in snapshots] == ["append", "append"]
    assert snapshots[1].summary["total-records"] == "3"
    # the second list carries the first manifest over unchanged
    manifests = table.current_snapshot().manifests(table.io)
    assert sorted((m.sequence_number, m.added_files_count, m.added_rows_count) for m in manifests) == [
        (1, 1, 2),
        (2, 1, 1),
    ]


def test_bounds_and_counts(setup):
    _, table = setup
    files = sorted((task.file for task in table.scan().plan_files()), key=lambda f: f.record_count)
    single, pair = files
    assert pair.record_count == 2
    assert pair.lower_bounds[1] == struct.pack("<q", 1)
    assert pair.upper_bounds[1] == struct.pack("<q", 2)
    assert pair.lower_bounds[5] == struct.pack("<d", -2.5)
    assert pair.upper_bounds[5] == struct.pack("<d", 1.5)
    assert pair.lower_bounds[4] == struct.pack("<i", 19723)  # 2024-01-01
    assert pair.upper_bounds[3] == struct.pack("<q", 1706745600000000)  # 2024-02-01 µs
    # strings carry no bounds (writes._bound_bytes)
    assert 2 not in pair.lower_bounds and 2 not in pair.upper_bounds
    # an all-null column has a null count and no bounds
    assert dict(single.null_value_counts)[2] == 1
    assert 5 not in single.lower_bounds
    assert dict(single.value_counts) == {1: 1, 2: 1, 3: 1, 4: 1, 5: 1}


def test_pyiceberg_reads_the_rows(setup):
    """An independent reader: needs the Iceberg field-ids in the Parquet schema."""
    _, table = setup
    data = table.scan().to_arrow().sort_by("id")
    assert data.column("id").to_pylist() == [1, 2, 3]
    assert data.column("name").to_pylist() == ["a", "b", None]
    assert data.column("x").to_pylist() == [1.5, -2.5, None]


def test_ctas_refused(setup):
    session, _ = setup
    with pytest.raises(Exception, match="CREATE TABLE ... AS SELECT"):
        run(session, f"CREATE TABLE {WORKSPACE}.ns.c AS SELECT 1 AS a")


def test_unmapped_type_refused(setup):
    session, _ = setup
    with pytest.raises(Exception, match="no exact Iceberg type"):
        run(session, f"CREATE TABLE {WORKSPACE}.ns.u (a SMALLINT)")
