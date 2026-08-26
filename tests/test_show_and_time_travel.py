"""SHOW SNAPSHOTS FOR / SHOW MANIFEST FOR / time travel, over a local Iceberg catalog.

tests/test_end_to_end_sql.py pushes plain SELECTs through opteryx-core against a
pyiceberg SqlCatalog. This file covers the rest of Tier 1's READ surface - the
statements that read a table's METADATA rather than its rows - because nothing
did, and one of them was broken:

  SHOW SNAPSHOTS FOR raised `'Snapshot' object has no attribute
  'operation_type'`. opteryx-core normalizes a commit history by reading fields
  off the snapshot BY ATTRIBUTE (opteryx.models.snapshot_history.
  normalize_snapshot), against opteryx_catalog's `Snapshot` dataclass. A
  pyiceberg `Snapshot` has none of `operation_type`, `author`, `user_created` or
  `commit_message`, spells its deleted-bytes counter `removed-files-size` where
  opteryx spells it `deleted-files-size`, holds every counter as a STRING, and
  types `schema_id` as an int where the output column is VARCHAR. dataset.
  IcebergSnapshot adapts all of that; these tests are what hold it.

SHOW MANIFEST FOR and both time-travel forms already worked - they are covered
here so they keep working, not because they were broken.

The expected values are arithmetic on ROWS_V1/ROWS_V2 below, not observed
output. Do not "fix" a failure by relaxing one.
"""

import datetime
import time

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

from opteryx_iceberg import IcebergMetastore
from opteryx_iceberg.dataset import IcebergSnapshot

# The pyiceberg catalog NAME must equal the Opteryx workspace prefix - see the
# note in test_end_to_end_sql.py and the README.
WORKSPACE = "tt"
TABLE = f"{WORKSPACE}.ns.t"

ROWS_V1 = pa.table({"i": pa.array([1, 2, 3], pa.int64())})
ROWS_V2 = pa.table({"i": pa.array([4, 5, 6, 7], pa.int64())})
V1_COUNT = 3
V2_COUNT = 4
TOTAL = V1_COUNT + V2_COUNT


@pytest.fixture(scope="module")
def fixture(tmp_path_factory):
    """A two-commit Iceberg table, registered as an Opteryx workspace.

    The commits are separated by a real sleep so their timestamps differ in
    whole SECONDS. `TIMESTAMP AS OF` takes a second-resolution literal, so two
    commits inside one second cannot be told apart by it and the point-in-time
    test would be asserting nothing.
    """
    root = tmp_path_factory.mktemp("iceberg-tt")
    warehouse = root / "warehouse"
    warehouse.mkdir()
    uri = f"sqlite:///{root / 'catalog.db'}"
    warehouse_uri = f"file://{warehouse}"

    writer = SqlCatalog(WORKSPACE, **{"uri": uri, "warehouse": warehouse_uri})
    writer.create_namespace("ns")
    table = writer.create_table("ns.t", schema=ROWS_V1.schema)
    table.append(ROWS_V1)
    time.sleep(2.2)
    table.append(ROWS_V2)

    snapshots = sorted(table.snapshots(), key=lambda s: s.timestamp_ms)

    register_workspace(
        WORKSPACE,
        OpteryxConnector,
        catalog=IcebergMetastore,
        catalog_type="sql",
        uri=uri,
        warehouse=warehouse_uri,
    )
    return {
        "session": opteryx.session(user="tests"),
        "table": table,
        "first": snapshots[0],
        "second": snapshots[1],
    }


def rows(session, sql):
    """Every row of a statement's result, as dicts."""
    tables = [m.to_arrow() for m in session.execute_to_morsels(sql) if m.num_rows]
    if not tables:
        return []
    combined = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    return combined.to_pylist()


def scalar(session, sql):
    """The single value of a one-row, one-column result."""
    result = rows(session, sql)
    assert len(result) == 1, f"expected one row, got {len(result)}"
    (value,) = result[0].values()
    return value


class TestTimeTravel:
    def test_a_plain_read_sees_every_commit(self, fixture):
        assert scalar(fixture["session"], f"SELECT COUNT(*) FROM {TABLE}") == TOTAL

    def test_version_as_of_an_id_sees_only_that_commit(self, fixture):
        """The first snapshot predates the second append, so it holds V1 only."""
        sql = f"SELECT COUNT(*) FROM {TABLE} VERSION AS OF {fixture['first'].snapshot_id}"
        assert scalar(fixture["session"], sql) == V1_COUNT

    def test_version_as_of_previous_walks_the_parent_link(self, fixture):
        """PREVIOUS resolves through `parent_snapshot_id`, so it must land on
        the first commit and not merely on 'some older snapshot'."""
        sql = f"SELECT COUNT(*) FROM {TABLE} VERSION AS OF PREVIOUS"
        assert scalar(fixture["session"], sql) == V1_COUNT

    def test_version_as_of_an_unknown_id_is_an_error(self, fixture):
        """Not silently the current snapshot - that would answer a question
        about a version that does not exist with live data."""
        with pytest.raises(Exception) as caught:
            rows(fixture["session"], f"SELECT COUNT(*) FROM {TABLE} VERSION AS OF 1234567")
        assert "1234567" in str(caught.value)

    def test_timestamp_as_of_between_the_commits_sees_the_first(self, fixture):
        # The first whole second STRICTLY AFTER commit one. Adding a sub-second
        # offset instead would be flaky: the literal is truncated to seconds,
        # so first-commit-at-x.300 plus 500ms formats back to x.000 - before
        # the commit it was meant to follow, and the read fails as "no data at
        # that date". Rounding up cannot land before commit one, and the
        # fixture's 2s gap keeps it before commit two.
        between = datetime.datetime.fromtimestamp(
            (fixture["first"].timestamp_ms // 1000) + 1
        )
        sql = (
            f"SELECT COUNT(*) FROM {TABLE} "
            f"TIMESTAMP AS OF '{between.strftime('%Y-%m-%d %H:%M:%S')}'"
        )
        assert scalar(fixture["session"], sql) == V1_COUNT

    def test_timestamp_as_of_after_every_commit_sees_them_all(self, fixture):
        later = datetime.datetime.fromtimestamp(
            fixture["second"].timestamp_ms / 1000 + 3600
        )
        sql = (
            f"SELECT COUNT(*) FROM {TABLE} "
            f"TIMESTAMP AS OF '{later.strftime('%Y-%m-%d %H:%M:%S')}'"
        )
        assert scalar(fixture["session"], sql) == TOTAL

    def test_timestamp_as_of_before_the_table_existed_is_an_error(self, fixture):
        """An empty result would claim the table was empty then; it did not
        exist. The engine raises, and that must reach the caller."""
        earlier = datetime.datetime.fromtimestamp(
            fixture["first"].timestamp_ms / 1000 - 3600
        )
        sql = (
            f"SELECT COUNT(*) FROM {TABLE} "
            f"TIMESTAMP AS OF '{earlier.strftime('%Y-%m-%d %H:%M:%S')}'"
        )
        with pytest.raises(Exception):
            rows(fixture["session"], sql)


class TestShowSnapshots:
    def test_one_row_per_commit_newest_first(self, fixture):
        result = rows(fixture["session"], f"SHOW SNAPSHOTS FOR {TABLE}")
        assert len(result) == 2
        assert [r["snapshot_id"] for r in result] == [
            fixture["second"].snapshot_id,
            fixture["first"].snapshot_id,
        ]

    def test_only_the_latest_commit_is_current(self, fixture):
        result = rows(fixture["session"], f"SHOW SNAPSHOTS FOR {TABLE}")
        assert [r["is_current"] for r in result] == [True, False]

    def test_the_parent_link_is_reported(self, fixture):
        newest, oldest = rows(fixture["session"], f"SHOW SNAPSHOTS FOR {TABLE}")
        assert newest["parent_snapshot_id"] == fixture["first"].snapshot_id
        assert oldest["parent_snapshot_id"] is None

    def test_the_operation_is_the_specs_lowercase_name(self, fixture):
        """'append', not 'Operation.APPEND' - the column is the Iceberg spec's
        own vocabulary, not a repr of pyiceberg's enum."""
        result = rows(fixture["session"], f"SHOW SNAPSHOTS FOR {TABLE}")
        assert [r["operation_type"] for r in result] == ["append", "append"]

    def test_counters_are_integers_with_the_committed_values(self, fixture):
        """Iceberg holds these as strings; arriving as ints with the right
        values is what proves the coercion runs and is not off by a commit."""
        newest, oldest = rows(fixture["session"], f"SHOW SNAPSHOTS FOR {TABLE}")
        assert newest["added_records"] == V2_COUNT
        assert oldest["added_records"] == V1_COUNT
        assert newest["total_records"] == TOTAL
        assert oldest["total_records"] == V1_COUNT
        assert newest["added_data_files"] == 1
        assert newest["total_data_files"] == 2
        for value in (newest["added_records"], newest["total_records"]):
            assert isinstance(value, int)

    def test_fields_iceberg_does_not_record_are_null_not_invented(self, fixture):
        """Iceberg's spec has no author, no commit message, and no user/system
        distinction. None reads as 'unknown', which is true; a placeholder
        string would read as recorded fact."""
        for row in rows(fixture["session"], f"SHOW SNAPSHOTS FOR {TABLE}"):
            assert row["author"] is None
            assert row["user_created"] is None
            assert row["commit_message"] is None

    def test_counters_absent_from_an_append_are_null_not_zero(self, fixture):
        """An APPEND records no deleted-* counters. None says 'this commit does
        not report it'; 0 would assert it deleted nothing."""
        for row in rows(fixture["session"], f"SHOW SNAPSHOTS FOR {TABLE}"):
            assert row["deleted_records"] is None
            assert row["deleted_data_files"] is None

    def test_committed_at_matches_the_snapshot_timestamp(self, fixture):
        newest, _ = rows(fixture["session"], f"SHOW SNAPSHOTS FOR {TABLE}")
        expected = datetime.datetime.fromtimestamp(
            fixture["second"].timestamp_ms / 1000, tz=datetime.timezone.utc
        )
        actual = newest["committed_at"]
        if actual.tzinfo is None:
            actual = actual.replace(tzinfo=datetime.timezone.utc)
        assert abs((actual - expected).total_seconds()) < 1


class TestShowManifest:
    def test_one_row_per_live_data_file(self, fixture):
        """Two appends, one file each, and neither compacted away."""
        result = rows(fixture["session"], f"SHOW MANIFEST FOR {TABLE}")
        assert len(result) == 2

    def test_record_counts_add_up_to_the_table(self, fixture):
        result = rows(fixture["session"], f"SHOW MANIFEST FOR {TABLE}")
        counts = sorted(r["record_count"] for r in result)
        assert counts == sorted([V1_COUNT, V2_COUNT])
        assert sum(counts) == TOTAL

    def test_the_bounds_are_the_real_decoded_iceberg_bounds(self, fixture):
        """Iceberg stores bounds as encoded BYTES. Seeing 1..3 and 4..7 here -
        the actual values of ROWS_V1/ROWS_V2 - is what proves they were
        decoded, and it is the same statistic the engine prunes on.

        They arrive as TEXT because SHOW MANIFEST renders min_values/max_values
        that way for every source (opteryx.models.manifest_io._bound_as_text):
        one row's bounds list holds one bound per field id, so a table whose
        columns are not all one type puts a str and an int in the same ARRAY
        column, which has one child type and cannot hold both. This table is
        single-column INT64, so it would have survived typed - the rendering is
        not conditional and neither is the assertion. What is asserted is
        unchanged: 1..3 and 4..7, not the raw bytes Iceberg stored.
        """
        result = rows(fixture["session"], f"SHOW MANIFEST FOR {TABLE}")
        bounds = sorted((r["min_values"][0], r["max_values"][0]) for r in result)
        assert bounds == [("1", "3"), ("4", "7")]


@pytest.fixture(scope="module")
def deleted(tmp_path_factory):
    from pyiceberg.expressions import GreaterThanOrEqual

    workspace = "ttdel"
    root = tmp_path_factory.mktemp("iceberg-tt-del")
    warehouse = root / "warehouse"
    warehouse.mkdir()
    uri = f"sqlite:///{root / 'catalog.db'}"
    warehouse_uri = f"file://{warehouse}"

    writer = SqlCatalog(workspace, **{"uri": uri, "warehouse": warehouse_uri})
    writer.create_namespace("ns")
    data = pa.table({"i": pa.array([1, 2, 3, 4, 5, 6], pa.int64())})
    table = writer.create_table("ns.t", schema=data.schema)
    table.append(data)
    table.delete(GreaterThanOrEqual("i", 4))  # removes 4,5,6 - leaves 3 rows

    register_workspace(
        workspace,
        OpteryxConnector,
        catalog=IcebergMetastore,
        catalog_type="sql",
        uri=uri,
        warehouse=warehouse_uri,
    )
    return opteryx.session(user="tests"), f"{workspace}.ns.t"


class TestShowSnapshotsAfterADelete:
    """The deleted-* counters, against a commit that actually records them.

    Every commit in the main fixture is an APPEND, and an APPEND reports no
    deleted-* counters at all - so the `removed-files-size` ->
    `deleted-files-size` rename is untested by it. A real delete is the only
    thing that populates those keys, and pyiceberg records it as an OVERWRITE
    carrying `removed-files-size`, `deleted-data-files` and `deleted-records`.
    """

    def test_the_delete_is_reported_as_an_overwrite(self, deleted):
        session, table = deleted
        newest = rows(session, f"SHOW SNAPSHOTS FOR {table}")[0]
        assert newest["operation_type"] == "overwrite"

    def test_the_deleted_counters_are_populated(self, deleted):
        """6 rows in one file went away; 3 were rewritten back."""
        session, table = deleted
        newest = rows(session, f"SHOW SNAPSHOTS FOR {table}")[0]
        assert newest["deleted_records"] == 6
        assert newest["deleted_data_files"] == 1
        assert newest["total_records"] == 3

    def test_removed_files_size_reaches_the_deleted_files_size_column(self, deleted):
        """The one counter whose Iceberg spelling differs from opteryx's. A
        missed rename shows up as None here, not as an error anywhere."""
        session, table = deleted
        newest = rows(session, f"SHOW SNAPSHOTS FOR {table}")[0]
        assert newest["deleted_files_size_in_bytes"] is not None
        assert newest["deleted_files_size_in_bytes"] > 0
        assert isinstance(newest["deleted_files_size_in_bytes"], int)

    def test_the_table_reads_back_at_the_post_delete_count(self, deleted):
        session, table = deleted
        assert scalar(session, f"SELECT COUNT(*) FROM {table}") == 3


@pytest.fixture(scope="module")
def tagged(tmp_path_factory):
    """A two-commit table with a TAG on the first commit and a BRANCH on it too.

    Both refs live in the same `metadata.refs` map, which is why the branch is
    here: it is what proves `list_tags` filters on ref TYPE rather than
    returning every ref it finds. Every Iceberg table also carries a `main`
    branch it never asked for, so a filter that is missing shows up as `main`
    tagging the current snapshot of every table anyone lists.
    """
    workspace = "tttag"
    root = tmp_path_factory.mktemp("iceberg-tt-tag")
    warehouse = root / "warehouse"
    warehouse.mkdir()
    uri = f"sqlite:///{root / 'catalog.db'}"
    warehouse_uri = f"file://{warehouse}"

    writer = SqlCatalog(workspace, **{"uri": uri, "warehouse": warehouse_uri})
    writer.create_namespace("ns")
    table = writer.create_table("ns.t", schema=ROWS_V1.schema)
    table.append(ROWS_V1)
    first = table.current_snapshot().snapshot_id
    table.append(ROWS_V2)

    manage = table.manage_snapshots()
    manage.create_tag(first, "release_one").commit()
    table.manage_snapshots().create_branch(first, "wip").commit()

    register_workspace(
        workspace,
        OpteryxConnector,
        catalog=IcebergMetastore,
        catalog_type="sql",
        uri=uri,
        warehouse=warehouse_uri,
    )
    return opteryx.session(user="tests"), f"{workspace}.ns.t", first


class TestTags:
    """SHOW SNAPSHOTS' tags column, and VERSION AS OF a tag.

    Both were AttributeError before IcebergMetastore had `list_tags` /
    `resolve_tag`: opteryx-core calls them on whatever catalog object the
    workspace was registered with, and an Iceberg table keeps its tags in
    `metadata.refs` rather than in a tags subcollection.
    """

    def test_the_tag_appears_against_the_snapshot_it_names(self, tagged):
        session, table, first = tagged
        result = rows(session, f"SHOW SNAPSHOTS FOR {table}")
        by_id = {row["snapshot_id"]: row["tags"] for row in result}
        assert by_id[first] == ["release_one"]

    def test_an_untagged_snapshot_lists_no_tags(self, tagged):
        session, table, first = tagged
        result = rows(session, f"SHOW SNAPSHOTS FOR {table}")
        others = [row["tags"] for row in result if row["snapshot_id"] != first]
        assert others == [[]]

    def test_branches_are_not_reported_as_tags(self, tagged):
        """`main` and `wip` are refs on this table; neither is a tag."""
        session, table, _ = tagged
        every_tag = [
            tag for row in rows(session, f"SHOW SNAPSHOTS FOR {table}") for tag in row["tags"]
        ]
        assert every_tag == ["release_one"]

    def test_version_as_of_the_tag_reads_that_snapshot(self, tagged):
        session, table, _ = tagged
        sql = f"SELECT COUNT(*) FROM {table} VERSION AS OF 'release_one'"
        assert scalar(session, sql) == V1_COUNT

    def test_version_as_of_a_branch_name_is_an_error(self, tagged):
        """A branch is not addressable by VERSION AS OF - opteryx has no way to
        say 'read this branch', and answering with the branch head would be a
        different question answered."""
        session, table, _ = tagged
        with pytest.raises(Exception):
            rows(session, f"SELECT COUNT(*) FROM {table} VERSION AS OF 'wip'")

    def test_version_as_of_an_unknown_tag_is_an_error(self, tagged):
        session, table, _ = tagged
        with pytest.raises(Exception):
            rows(session, f"SELECT COUNT(*) FROM {table} VERSION AS OF 'no_such_tag'")

    def test_a_tag_is_matched_as_spelled(self, tagged):
        """The native catalog lowercases tag names on the way in; an Iceberg
        ref name is whatever wrote it. Resolving `RELEASE_ONE` against a ref
        named `release_one` would mean opteryx-iceberg normalizing names it
        does not own."""
        session, table, _ = tagged
        with pytest.raises(Exception):
            rows(session, f"SELECT COUNT(*) FROM {table} VERSION AS OF 'RELEASE_ONE'")


class TestIcebergSnapshotAdapter:
    """Unit-level checks on the edges a real catalog will not readily produce.

    The `removed-files-size` rename is covered against a real delete commit
    above; what is left here is the malformed input a healthy catalog never
    emits and an unhealthy one might.
    """

    def test_an_unreadable_counter_is_none_rather_than_zero(self):
        class FakeSummary(dict):
            operation = None

        snapshot = IcebergSnapshot(
            type(
                "S",
                (),
                {
                    "snapshot_id": 1,
                    "parent_snapshot_id": None,
                    "sequence_number": 1,
                    "timestamp_ms": 0,
                    "schema_id": 3,
                    "summary": FakeSummary({"added-records": "not-a-number"}),
                },
            )()
        )
        assert snapshot.summary["added-records"] is None
        # VARCHAR column, int in Iceberg.
        assert snapshot.schema_id == "3"
