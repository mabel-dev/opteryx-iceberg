"""Tier 2 writes through a real Iceberg REST catalog over S3 (Lakekeeper + Silo,
scripts/lakekeeper.sh): CREATE SCHEMA, CREATE TABLE and two INSERTs from
Opteryx, then the table read back by Opteryx and by plain pyiceberg.

The data files, manifests and manifest lists land in S3 through
opteryx-iceberg's writer (opteryx_catalog's S3FileIO under opteryx-core's
endpoint and key); the commits go through Lakekeeper's REST API.
"""

import uuid

import pytest
from _lakekeeper import WAREHOUSES
from _lakekeeper import require_stack
from _lakekeeper import rest_properties
from _lakekeeper import rows
from _lakekeeper import writable
from _lakekeeper import writer as make_writer

require_stack()

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

from opteryx_iceberg import UnrestrictedIcebergMetastore

NAMESPACE = f"ci_w_{uuid.uuid4().hex[:12]}"


def run(session, sql):
    return list(session.execute_to_morsels(sql))


@pytest.fixture(scope="module", params=sorted(WAREHOUSES))
def fixture(request):
    workspace = request.param
    register_workspace(
        workspace,
        OpteryxConnector,
        catalog=UnrestrictedIcebergMetastore,
        catalog_type="rest",
        **rest_properties(workspace),
    )
    session = opteryx.session(user="tests")
    name = f"{workspace}.{NAMESPACE}.t"
    run(session, f"CREATE SCHEMA {workspace}.{NAMESPACE}")
    run(session, f"CREATE TABLE {name} (id BIGINT, name VARCHAR, score DOUBLE)")
    run(session, f"INSERT INTO {name} SELECT * FROM (VALUES (1, 'a', 1.5), (2, 'b', 2.5)) AS v(id, name, score)")
    run(session, f"INSERT INTO {name} SELECT * FROM (VALUES (3, 'c', 3.5)) AS v(id, name, score)")
    catalog = make_writer(workspace)
    yield {"session": session, "name": name, "table": catalog.load_table(f"{NAMESPACE}.t")}
    catalog.purge_table(f"{NAMESPACE}.t")
    catalog.drop_namespace(NAMESPACE)


def test_opteryx_reads_back(fixture):
    assert rows(fixture["session"], f"SELECT id, name, score FROM {fixture['name']} ORDER BY id") == [
        {"id": 1, "name": "a", "score": 1.5},
        {"id": 2, "name": "b", "score": 2.5},
        {"id": 3, "name": "c", "score": 3.5},
    ]


def test_two_appends_committed(fixture):
    table = fixture["table"]
    first, second = sorted(table.snapshots(), key=lambda s: s.sequence_number)
    assert (first.sequence_number, second.sequence_number) == (1, 2)
    assert second.parent_snapshot_id == first.snapshot_id
    assert table.current_snapshot().snapshot_id == second.snapshot_id
    assert table.current_snapshot().summary["total-records"] == "3"


def test_files_are_in_the_table_location(fixture):
    table = fixture["table"]
    location = table.metadata.location.rstrip("/")
    paths = [task.file.file_path for task in writable(table).scan().plan_files()]
    assert len(paths) == 2
    assert all(path.startswith(f"{location}/data/") for path in paths)


def test_pyiceberg_reads_the_rows(fixture):
    data = writable(fixture["table"]).scan().to_arrow().sort_by("id")
    assert data.column("id").to_pylist() == [1, 2, 3]
    assert data.column("name").to_pylist() == ["a", "b", "c"]
