"""Tier 1 reads through a REAL Iceberg REST catalog: Lakekeeper, over S3.

Every other test in this directory runs against a pyiceberg SqlCatalog on local
disk, so none of them crosses the wire: the REST protocol, the load-table
response's `config` (which is where a REST catalog tells the client its S3
endpoint, credentials and even its FileIO implementation), and s3:// metadata
and data reads through opteryx-core's S3 client are all untested there. This
file is that path, end to end, against a stack started by
scripts/lakekeeper.sh (Lakekeeper + Postgres + Silo S3).

Every test runs against both of the stack's warehouses:

  plain    remote signing off - a table load serves only the S3 endpoint.
  default  Lakekeeper's default, remote signing on - a table load also serves
           a signer and `py-io-impl: pyiceberg.io.fsspec.FsspecFileIO`.
           pyiceberg lets that override the catalog's own `py-io-impl`, so
           `table.io` is FsspecFileIO; opteryx-iceberg must read manifests
           through OpteryxFileIO regardless (IcebergMetastore.load_dataset).

The stack only exists where it was started, so the module SKIPS when
LAKEKEEPER_URI is unset (a dev machine without Docker). The CI job that starts
it sets LAKEKEEPER_REQUIRED=1, which turns that skip into a failure: CI must
never pass this file by skipping it.

Tables are written by plain pyiceberg RestCatalog, independently of
opteryx-iceberg, into a namespace unique to the run, so the module can be
re-run against a long-lived local stack. The expected values are arithmetic on
ROWS_V1/ROWS_V2, not observed output. Do not "fix" a failure by relaxing one.
"""

import os
import time
import uuid

import pyarrow as pa
import pytest

if not os.environ.get("LAKEKEEPER_URI"):
    if os.environ.get("LAKEKEEPER_REQUIRED"):
        raise RuntimeError(
            "LAKEKEEPER_REQUIRED is set but LAKEKEEPER_URI is not - load scripts/lakekeeper.env."
        )
    pytest.skip(
        "no Lakekeeper stack (LAKEKEEPER_URI unset) - see scripts/lakekeeper.sh",
        allow_module_level=True,
    )

from pyiceberg.catalog import load_catalog

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

from opteryx_iceberg import IcebergMetastore
from opteryx_iceberg import fileio

NAMESPACE = f"ci_{uuid.uuid4().hex[:12]}"

# workspace prefix -> the env var naming its Lakekeeper warehouse
WAREHOUSES = {
    "lakekeeper": "LAKEKEEPER_WAREHOUSE",
    "lakekeeper_default": "LAKEKEEPER_DEFAULT_WAREHOUSE",
}

ROWS_V1 = pa.table(
    {
        "id": pa.array([1, 2, 3], pa.int64()),
        "name": pa.array(["a", "b", "c"]),
        "score": pa.array([1.5, 2.5, 3.5], pa.float64()),
    }
)
ROWS_V2 = pa.table(
    {
        "id": pa.array([4, 5, 6, 7], pa.int64()),
        "name": pa.array(["d", "e", "f", "g"]),
        "score": pa.array([10.5, 20.5, 30.5, 40.5], pa.float64()),
    }
)
V1_COUNT = 3
TOTAL = 7


def _rest_properties(workspace):
    return {
        "uri": f"{os.environ['LAKEKEEPER_URI']}/catalog",
        "warehouse": os.environ[WAREHOUSES[workspace]],
    }


def rows(session, sql):
    tables = [m.to_arrow() for m in session.execute_to_morsels(sql) if m.num_rows]
    if not tables:
        return []
    combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    return combined.to_pylist()


def scalar(session, sql):
    result = rows(session, sql)
    assert len(result) == 1
    (value,) = result[0].values()
    return value


@pytest.fixture(scope="module", params=sorted(WAREHOUSES))
def fixture(request):
    """A two-commit table written through Lakekeeper, read as a workspace.

    The commits are separated by a sleep so their timestamps differ, keeping
    SHOW SNAPSHOTS' newest-first order a real assertion.
    """
    workspace = request.param
    # The writer is pyiceberg's own FileIO - whatever the warehouse serves
    # (pyarrow for `plain`, fsspec + Lakekeeper's signer for `default`) - with
    # the key given explicitly. The reader is opteryx-core's S3 client,
    # configured from the process environment with the same static key.
    writer = load_catalog(
        f"{workspace}_writer",
        type="rest",
        **_rest_properties(workspace),
        **{
            "s3.endpoint": os.environ["AWS_S3_ENDPOINT"],
            "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "s3.region": os.environ["AWS_REGION"],
            "s3.path-style-access": "true",
        },
    )
    writer.create_namespace(NAMESPACE)
    table = writer.create_table(f"{NAMESPACE}.t", schema=ROWS_V1.schema)
    table.append(ROWS_V1)
    time.sleep(1.2)
    table.append(ROWS_V2)
    snapshots = sorted(table.snapshots(), key=lambda s: s.timestamp_ms)

    register_workspace(
        workspace,
        OpteryxConnector,
        catalog=IcebergMetastore,
        catalog_type="rest",
        **_rest_properties(workspace),
    )
    yield {
        "workspace": workspace,
        "name": f"{workspace}.{NAMESPACE}.t",
        "session": opteryx.session(user="tests"),
        "table": table,
        "first": snapshots[0],
        "last": snapshots[-1],
    }

    writer.drop_table(f"{NAMESPACE}.t")
    writer.drop_namespace(NAMESPACE)


class TestWhatLakekeeperServes:
    """Preconditions: without these the tests below would pass vacuously."""

    def test_the_same_s3_endpoint_is_served(self, fixture):
        """opteryx_iceberg.fileio refuses a read whose catalog properties name
        a different S3 endpoint from opteryx-core's. These reads pass only
        because the two agree - this proves the endpoint is actually served
        (so the check runs), rather than absent (so it is skipped)."""
        served = fixture["table"].io.properties["s3.endpoint"].rstrip("/")
        assert served == os.environ["AWS_S3_ENDPOINT"].rstrip("/")

    def test_the_served_py_io_impl(self, fixture):
        served = fixture["table"].io.properties.get("py-io-impl")
        if fixture["workspace"] == "lakekeeper_default":
            assert served == "pyiceberg.io.fsspec.FsspecFileIO"
        else:
            assert served is None


class TestMetadataIsReadThroughOpteryxCore:
    def test_manifests_are_read_by_opteryx_core(self, fixture, monkeypatch):
        """The manifest list and manifests must be fetched by opteryx-core's
        reader (fileio._ObjectReader, behind OpteryxFileIO and its identity
        check) - on the `default` warehouse too, where pyiceberg's `table.io`
        is the served FsspecFileIO.

        Driven through `manifest_bytes` rather than SQL: opteryx-core caches
        the decoded manifest per manifest list per process, so a query may
        legitimately read nothing."""
        read = []
        original = fileio._ObjectReader.read

        def spy(self, location):
            read.append(location)
            return original(self, location)

        monkeypatch.setattr(fileio._ObjectReader, "read", spy)
        workspace = fixture["workspace"]
        dataset = IcebergMetastore(
            workspace, catalog_type="rest", **_rest_properties(workspace)
        ).load_dataset(f"{NAMESPACE}.t")

        assert dataset.manifest_bytes() is not None
        assert fixture["last"].manifest_list in read
        manifests = [m.manifest_path for m in fixture["last"].manifests(fixture["table"].io)]
        assert manifests
        assert set(manifests) <= set(read)


class TestReads:
    def test_count(self, fixture):
        assert scalar(fixture["session"], f"SELECT COUNT(*) FROM {fixture['name']}") == TOTAL

    def test_every_row_and_value(self, fixture):
        expected = pa.concat_tables([ROWS_V1, ROWS_V2]).to_pylist()
        result = rows(
            fixture["session"], f"SELECT id, name, score FROM {fixture['name']} ORDER BY id"
        )
        assert result == expected

    def test_predicate(self, fixture):
        """Matches only the second commit's data file, so a bounds-pruned scan
        and a full scan must agree on it."""
        result = rows(
            fixture["session"],
            f"SELECT id FROM {fixture['name']} WHERE score > 15 ORDER BY id",
        )
        assert [r["id"] for r in result] == [5, 6, 7]

    def test_information_schema_columns(self, fixture):
        result = rows(
            fixture["session"],
            f"SELECT * FROM {fixture['workspace']}.information_schema.columns",
        )
        ours = [r["column_name"] for r in result if r["table_schema"] == NAMESPACE]
        assert sorted(ours) == ["id", "name", "score"]


class TestTimeTravelAndSnapshots:
    def test_version_as_of_the_first_snapshot(self, fixture):
        sql = (
            f"SELECT COUNT(*) FROM {fixture['name']} "
            f"VERSION AS OF {fixture['first'].snapshot_id}"
        )
        assert scalar(fixture["session"], sql) == V1_COUNT

    def test_show_snapshots_newest_first(self, fixture):
        result = rows(fixture["session"], f"SHOW SNAPSHOTS FOR {fixture['name']}")
        assert len(result) == 2
        assert result[0]["snapshot_id"] == fixture["last"].snapshot_id
        assert result[1]["snapshot_id"] == fixture["first"].snapshot_id
