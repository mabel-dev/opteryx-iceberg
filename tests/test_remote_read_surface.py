"""The read surface a REMOTE Iceberg catalog exercises but a local one does not.

tests/test_end_to_end_sql.py and tests/test_show_and_time_travel.py both run
against a pyiceberg SqlCatalog, and pyiceberg writes a narrower range of tables
than Spark/Flink/Trino do. Three defects lived in that gap - each one found by
auditing against a real catalog, not by reading code:

  1. MERGE-ON-READ DELETES were read as if they did not exist. Iceberg v2 can
     express a delete either by rewriting the data file (copy-on-write) or by
     committing a DELETE FILE beside it (merge-on-read) that the reader must
     subtract at scan time. `scan` took `task.file` and never looked at
     `task.delete_files`, so every deleted row came back as live data - no
     error, no warning, just wrong counts. pyiceberg has NO merge-on-read write
     path ("Merge on read is not yet supported, falling back to copy-on-write"),
     so no local fixture can produce the condition and no local test would ever
     have caught it. The engines that write remote catalogs all can. `scan` now
     refuses such a table; see `_reject_merge_on_read`.

  2. TIME TRAVEL REPORTED TODAY'S COLUMNS. `schema()` ignored the schema_id it
     was handed and always returned the table's current schema, so reading a
     snapshot taken before an ADD COLUMN listed the column that did not exist
     yet. Rows were right; the schema was not.

  3. information_schema was UNREACHABLE. It walks a catalog through
     list_collections -> list_datasets -> list_views/list_triggers; three of
     those did not exist on IcebergMetastore, and `list_datasets` returned
     namespace-QUALIFIED names where the contract (and the native catalog) is
     bare ones, which turned every lookup built from a listing into
     `DatasetNotFound: 'ns.ns.t'`.
"""

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import StringType

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

from opteryx_iceberg import IcebergMetastore
from opteryx_iceberg.dataset import IcebergDataset


def _catalog(tmp_path, workspace):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    uri = f"sqlite:///{tmp_path / 'catalog.db'}"
    warehouse_uri = f"file://{warehouse}"
    catalog = SqlCatalog(workspace, **{"uri": uri, "warehouse": warehouse_uri})
    catalog.create_namespace("ns")
    return catalog, uri, warehouse_uri


def _register(workspace, uri, warehouse_uri):
    register_workspace(
        workspace,
        OpteryxConnector,
        catalog=IcebergMetastore,
        catalog_type="sql",
        uri=uri,
        warehouse=warehouse_uri,
    )
    return opteryx.session(user="tests")


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


class TestMergeOnReadIsRefused:
    """The guard, driven by a SYNTHETIC delete file.

    pyiceberg cannot write one (see the module docstring), so the scan task is
    given delete files directly. That is the whole condition the guard tests
    for - `plan_files` yielding a task whose `delete_files` is non-empty - and
    it is what a Spark-written merge-on-read table produces.
    """

    @pytest.fixture
    def table(self, tmp_path):
        catalog, uri, warehouse_uri = _catalog(tmp_path, "mor")
        data = pa.table({"i": pa.array([1, 2, 3, 4, 5, 6], pa.int64())})
        catalog.create_table("ns.t", schema=data.schema).append(data)
        return _register("mor", uri, warehouse_uri), "mor.ns.t"

    def test_a_copy_on_write_table_still_reads(self, table):
        """The guard must not fire on the ordinary case - every table
        pyiceberg writes has empty delete_files."""
        session, name = table
        assert scalar(session, f"SELECT COUNT(*) FROM {name}") == 6

    def test_delete_files_on_a_task_refuse_the_scan(self, table, monkeypatch):
        session, name = table
        original = IcebergDataset.scan

        def scan_with_deletes(self, *args, **kwargs):
            real_plan = self._table.scan

            def planned(*a, **k):
                table_scan = real_plan(*a, **k)

                class _Tasks:
                    def plan_files(inner):
                        for task in table_scan.plan_files():
                            # A positional delete file, as Spark would commit.
                            task.delete_files = frozenset({"delete-0001.parquet"})
                            yield task

                return _Tasks()

            monkeypatch.setattr(self._table, "scan", planned)
            return original(self, *args, **kwargs)

        monkeypatch.setattr(IcebergDataset, "scan", scan_with_deletes)

        with pytest.raises(Exception) as caught:
            rows(session, f"SELECT COUNT(*) FROM {name}")
        message = str(caught.value)
        assert "merge-on-read" in message
        # The failure must name the cause, not just fail: an operator seeing
        # this needs to know the table is unsupported, not that opteryx broke.
        assert "deleted rows as live data" in message

    def test_the_guard_says_nothing_when_delete_files_are_empty(self, table):
        """An empty (not absent) delete_files set is the copy-on-write case and
        must pass - guarding on presence of the attribute rather than on its
        contents would refuse every table."""
        session, name = table
        assert scalar(session, f"SELECT COUNT(*) FROM {name}") == 6


@pytest.fixture(scope="module")
def evolved(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("iceberg-evo")
    catalog, uri, warehouse_uri = _catalog(tmp_path, "evo")
    table = catalog.create_table(
        "ns.e", schema=pa.table({"a": pa.array([1], pa.int64())}).schema
    )
    table.append(pa.table({"a": pa.array([1, 2], pa.int64())}))
    before = table.current_snapshot().snapshot_id
    with table.update_schema() as update:
        update.add_column("b", StringType())
    table.append(
        pa.table({"a": pa.array([3], pa.int64()), "b": pa.array(["x"])})
    )
    return _register("evo", uri, warehouse_uri), "evo.ns.e", before


class TestTimeTravelUsesTheHistoricalSchema:
    def test_the_current_read_sees_the_added_column(self, evolved):
        session, name, _ = evolved
        assert sorted(rows(session, f"SELECT * FROM {name}")[0].keys()) == ["a", "b"]

    def test_a_pre_evolution_snapshot_does_not_see_it(self, evolved):
        """The column did not exist at that commit. Reporting it is a claim
        about history that is simply untrue."""
        session, name, before = evolved
        result = rows(session, f"SELECT * FROM {name} VERSION AS OF {before}")
        assert sorted(result[0].keys()) == ["a"]

    def test_the_pre_evolution_rows_are_still_right(self, evolved):
        """The schema fix must not have cost the row-level correctness that
        already worked."""
        session, name, before = evolved
        assert scalar(session, f"SELECT COUNT(*) FROM {name} VERSION AS OF {before}") == 2


@pytest.fixture(scope="module")
def listed(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("iceberg-list")
    catalog, uri, warehouse_uri = _catalog(tmp_path, "meta")
    catalog.create_namespace("other")
    data = pa.table({"a": pa.array([1], pa.int64())})
    catalog.create_table("ns.t", schema=data.schema).append(data)
    catalog.create_table("ns.u", schema=data.schema).append(data)
    catalog.create_table("other.v", schema=data.schema).append(data)
    metastore = IcebergMetastore(
        workspace="meta", catalog_type="sql", uri=uri, warehouse=warehouse_uri
    )
    return _register("meta", uri, warehouse_uri), metastore


class TestCatalogListingSurface:
    def test_list_collections_returns_dotted_namespaces(self, listed):
        _, metastore = listed
        assert sorted(metastore.list_collections()) == ["ns", "other"]

    def test_list_datasets_returns_bare_names(self, listed):
        """Not "ns.t" - information_schema pairs the name with the collection
        it asked for, so a qualified name here becomes `ns.ns.t`."""
        _, metastore = listed
        assert sorted(metastore.list_datasets("ns")) == ["t", "u"]
        assert sorted(metastore.list_datasets("other")) == ["v"]

    def test_views_and_triggers_are_empty_not_errors(self, listed):
        """Empty is the truthful answer for a Tier 1 reader; raising would take
        information_schema down over a table listing that is otherwise fine."""
        _, metastore = listed
        assert list(metastore.list_views("ns")) == []
        assert list(metastore.list_triggers("ns.t")) == []

    def test_information_schema_tables_lists_every_table(self, listed):
        session, _ = listed
        result = rows(session, "SELECT * FROM meta.information_schema.tables")
        names = sorted(r["table_name"] for r in result)
        assert names == ["t", "u", "v"]

    def test_information_schema_tables_reports_the_owning_namespace(self, listed):
        session, _ = listed
        result = rows(session, "SELECT * FROM meta.information_schema.tables")
        by_name = {r["table_name"]: r["table_schema"] for r in result}
        assert by_name == {"t": "ns", "u": "ns", "v": "other"}

    def test_information_schema_columns_resolves_each_table(self, listed):
        """This is the query the qualified-name bug broke: it loads every
        listed dataset, so a name that does not resolve is DatasetNotFound."""
        session, _ = listed
        result = rows(session, "SELECT * FROM meta.information_schema.columns")
        assert sorted({r["table_name"] for r in result}) == ["t", "u", "v"]
        assert all(r["column_name"] == "a" for r in result)
