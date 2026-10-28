"""The Tier 1 read surface over Lakekeeper, beyond plain appended tables.

tests/test_lakekeeper_rest.py covers one unpartitioned two-commit table; the
query battery (tests/test_lakekeeper_queries.py) covers SQL breadth over
appended data. This file covers the table SHAPES and metadata a real REST
catalog serves that neither does: partitioned tables, schema evolution, a
copy-on-write delete, tags, point-in-time reads, several namespaces, the
refusals, and which scan path the data files take.

Every test runs against both warehouses (tests/_lakekeeper.py); skips without
the stack, fails without it under LAKEKEEPER_REQUIRED. Tables are written by
plain pyiceberg RestCatalog. The expected values are arithmetic on the rows
written below, not observed output - do not "fix" a failure by relaxing one.
"""

import datetime
import decimal
import time
import uuid

import pyarrow as pa
import pytest
from _lakekeeper import WAREHOUSES
from _lakekeeper import require_stack
from _lakekeeper import rest_properties
from _lakekeeper import rows
from _lakekeeper import scalar
from _lakekeeper import writable
from _lakekeeper import writer as make_writer

require_stack()

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import DatasetNotFoundError
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import DecimalType
from pyiceberg.types import LongType
from pyiceberg.types import NestedField
from pyiceberg.types import StringType
from pyiceberg.types import TimestampType

from opteryx_iceberg import IcebergMetastore

RUN = f"ci_{uuid.uuid4().hex[:12]}"
MAIN = f"{RUN}_surface"
OTHER = f"{RUN}_other"

# ── events: partitioned by identity(kind) and day(at) ────────────────────────
KINDS = ("a", "b", "c")
DAY_ZERO = datetime.datetime(2026, 1, 1)
EVENTS = [
    {
        "id": i,
        "kind": KINDS[i % 3],
        "at": DAY_ZERO + datetime.timedelta(days=i % 5, minutes=i),
        "amount": decimal.Decimal(i) + decimal.Decimal("0.25"),
    }
    for i in range(1, 31)
]
EVENTS_SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "kind", StringType(), required=False),
    NestedField(3, "at", TimestampType(), required=False),
    NestedField(4, "amount", DecimalType(10, 2), required=False),
)
EVENTS_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="kind"),
    PartitionField(source_id=3, field_id=1001, transform=DayTransform(), name="at_day"),
)
EVENTS_ARROW = pa.table(
    {
        "id": pa.array([r["id"] for r in EVENTS], pa.int64()),
        "kind": pa.array([r["kind"] for r in EVENTS], pa.string()),
        "at": pa.array([r["at"] for r in EVENTS], pa.timestamp("us")),
        "amount": pa.array([r["amount"] for r in EVENTS], pa.decimal128(10, 2)),
    }
)
PARTITIONS = {(r["kind"], r["at"].date()) for r in EVENTS}

# ── history: three appends, a tag, then a copy-on-write delete ───────────────
BATCHES = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]
DELETED_ID = 2


def _ids(values):
    return pa.table({"id": pa.array(values, pa.int64())})


@pytest.fixture(scope="module", params=sorted(WAREHOUSES))
def surface(request):
    workspace = request.param
    writer = make_writer(workspace)
    writer.create_namespace(MAIN)
    writer.create_namespace(OTHER)

    events = writable(writer.create_table(
        f"{MAIN}.events", schema=EVENTS_SCHEMA, partition_spec=EVENTS_SPEC
    ))
    events.append(EVENTS_ARROW)

    evolving = writable(writer.create_table(
        f"{MAIN}.evolving",
        schema=pa.schema([pa.field("id", pa.int64()), pa.field("a", pa.string())]),
    ))
    evolving.append(pa.table({"id": pa.array([1, 2], pa.int64()), "a": pa.array(["x", "y"])}))
    before_evolution = evolving.current_snapshot().snapshot_id
    with evolving.update_schema() as update:
        update.add_column("b", StringType())
    evolving.append(
        pa.table(
            {"id": pa.array([3], pa.int64()), "a": pa.array(["z"]), "b": pa.array(["new"])}
        )
    )

    # Commits a whole SECOND apart: TIMESTAMP AS OF takes a second-resolution
    # literal, so commits inside one second could not be told apart by it.
    history = writable(writer.create_table(f"{MAIN}.history", schema=_ids([]).schema))
    commits = []
    for batch in BATCHES:
        history.append(_ids(batch))
        commits.append(history.current_snapshot())
        time.sleep(1.2)
    history.manage_snapshots().create_tag(commits[0].snapshot_id, "first_batch").commit()
    history.delete(EqualTo("id", DELETED_ID))
    after_delete = history.current_snapshot()

    elsewhere = writable(writer.create_table(f"{OTHER}.elsewhere", schema=_ids([]).schema))
    elsewhere.append(_ids([1]))

    register_workspace(
        workspace,
        OpteryxConnector,
        catalog=IcebergMetastore,
        catalog_type="rest",
        **rest_properties(workspace),
    )
    yield {
        "workspace": workspace,
        "session": opteryx.session(user="tests"),
        "main": f"{workspace}.{MAIN}",
        "other": f"{workspace}.{OTHER}",
        "before_evolution": before_evolution,
        "commits": commits,
        "after_delete": after_delete,
        "writer": writer,
    }

    for name in ("events", "evolving", "history"):
        writer.drop_table(f"{MAIN}.{name}")
    writer.drop_table(f"{OTHER}.elsewhere")
    writer.drop_namespace(MAIN)
    writer.drop_namespace(OTHER)


class TestPartitionedTable:
    def test_one_data_file_per_partition(self, surface):
        """The table really is partitioned - otherwise the tests below are
        reading an ordinary table."""
        result = rows(surface["session"], f"SHOW MANIFEST FOR {surface['main']}.events")
        assert len(result) == len(PARTITIONS)
        assert sum(r["record_count"] for r in result) == len(EVENTS)

    def test_every_row(self, surface):
        result = rows(
            surface["session"], f"SELECT id, kind, at, amount FROM {surface['main']}.events ORDER BY id"
        )
        assert result == EVENTS

    def test_predicate_on_the_identity_partition(self, surface):
        result = rows(
            surface["session"],
            f"SELECT id FROM {surface['main']}.events WHERE kind = 'b' ORDER BY id",
        )
        assert [r["id"] for r in result] == [r["id"] for r in EVENTS if r["kind"] == "b"]

    def test_predicate_on_the_day_partition(self, surface):
        cutoff = DAY_ZERO + datetime.timedelta(days=3)
        result = rows(
            surface["session"],
            f"SELECT id FROM {surface['main']}.events "
            f"WHERE at >= '{cutoff:%Y-%m-%d %H:%M:%S}'::TIMESTAMP ORDER BY id",
        )
        assert [r["id"] for r in result] == [r["id"] for r in EVENTS if r["at"] >= cutoff]

    def test_aggregate_per_partition_value(self, surface):
        result = rows(
            surface["session"],
            f"SELECT kind, COUNT(*) AS n, SUM(amount) AS total FROM {surface['main']}.events "
            "GROUP BY kind ORDER BY kind",
        )
        expected = [
            {
                "kind": kind,
                "n": sum(1 for r in EVENTS if r["kind"] == kind),
                "total": sum(r["amount"] for r in EVENTS if r["kind"] == kind),
            }
            for kind in KINDS
        ]
        assert result == expected

    def test_predicate_on_a_non_partition_column(self, surface):
        result = rows(
            surface["session"],
            f"SELECT COUNT(*) AS n FROM {surface['main']}.events WHERE amount > 20",
        )
        assert result == [{"n": sum(1 for r in EVENTS if r["amount"] > 20)}]


class TestSchemaEvolution:
    def test_the_current_schema_null_fills_older_rows(self, surface):
        result = rows(
            surface["session"], f"SELECT id, a, b FROM {surface['main']}.evolving ORDER BY id"
        )
        assert result == [
            {"id": 1, "a": "x", "b": None},
            {"id": 2, "a": "y", "b": None},
            {"id": 3, "a": "z", "b": "new"},
        ]

    def test_time_travel_reports_the_historical_schema(self, surface):
        result = rows(
            surface["session"],
            f"SELECT * FROM {surface['main']}.evolving "
            f"VERSION AS OF {surface['before_evolution']} ORDER BY id",
        )
        assert result == [{"id": 1, "a": "x"}, {"id": 2, "a": "y"}]


class TestHistory:
    def test_the_delete_is_applied(self, surface):
        result = rows(surface["session"], f"SELECT id FROM {surface['main']}.history ORDER BY id")
        expected = sorted(i for batch in BATCHES for i in batch if i != DELETED_ID)
        assert [r["id"] for r in result] == expected

    def test_before_the_delete_the_row_is_there(self, surface):
        last_append = surface["commits"][-1].snapshot_id
        sql = f"SELECT COUNT(*) FROM {surface['main']}.history VERSION AS OF {last_append}"
        assert scalar(surface["session"], sql) == sum(len(b) for b in BATCHES)

    def test_version_as_of_a_tag(self, surface):
        sql = f"SELECT COUNT(*) FROM {surface['main']}.history VERSION AS OF 'first_batch'"
        assert scalar(surface["session"], sql) == len(BATCHES[0])

    def test_timestamp_as_of_between_commits(self, surface):
        """A point in time after the second commit and before the third reads
        the second commit."""
        # The literal is LOCAL time, truncated to seconds - the same form as
        # tests/test_show_and_time_travel.py: the first whole second strictly
        # after commit two, which the fixture's 1.2s gap keeps before commit three.
        between = datetime.datetime.fromtimestamp(
            (surface["commits"][1].timestamp_ms // 1000) + 1
        )
        sql = (
            f"SELECT COUNT(*) FROM {surface['main']}.history "
            f"TIMESTAMP AS OF '{between:%Y-%m-%d %H:%M:%S}'"
        )
        assert scalar(surface["session"], sql) == len(BATCHES[0]) + len(BATCHES[1])

    def test_show_snapshots_reports_the_delete_as_an_overwrite(self, surface):
        """A pyiceberg row delete commits an `overwrite` (see the README)."""
        result = rows(surface["session"], f"SHOW SNAPSHOTS FOR {surface['main']}.history")
        assert result[0]["snapshot_id"] == surface["after_delete"].snapshot_id
        assert result[0]["operation_type"] == "overwrite"
        assert [r["operation_type"] for r in result[-len(BATCHES):]] == ["append"] * len(BATCHES)


class TestNamespaces:
    def test_information_schema_tables_spans_namespaces(self, surface):
        result = rows(
            surface["session"],
            f"SELECT table_schema, table_name FROM {surface['workspace']}.information_schema.tables",
        )
        ours = sorted(
            (r["table_schema"], r["table_name"]) for r in result if r["table_schema"] in (MAIN, OTHER)
        )
        assert ours == sorted(
            [(MAIN, t) for t in ("events", "evolving", "history")] + [(OTHER, "elsewhere")]
        )

    def test_a_table_in_another_namespace_reads(self, surface):
        assert scalar(surface["session"], f"SELECT COUNT(*) FROM {surface['other']}.elsewhere") == 1


class TestRefusals:
    def test_a_missing_table_is_dataset_not_found(self, surface):
        with pytest.raises(DatasetNotFoundError):
            rows(surface["session"], f"SELECT * FROM {surface['main']}.no_such_table")


class TestNested:
    def test_a_nested_column_reads(self, surface):
        """A list of a plain scalar is an ARRAY (tests/test_nested_columns.py
        covers every nested shape locally); here it is read through the REST
        catalog, without the nested column and with it."""
        writer = surface["writer"]
        nested = writable(writer.create_table(
            f"{MAIN}.nested",
            schema=pa.schema([pa.field("id", pa.int64()), pa.field("tags", pa.list_(pa.string()))]),
        ))
        try:
            nested.append(
                pa.table({"id": pa.array([1], pa.int64()), "tags": pa.array([["p", "q"]])})
            )
            assert rows(surface["session"], f"SELECT id FROM {surface['main']}.nested") == [{"id": 1}]
            assert rows(surface["session"], f"SELECT id, tags FROM {surface['main']}.nested") == [
                {"id": 1, "tags": ["p", "q"]}
            ]
        finally:
            writer.drop_table(f"{MAIN}.nested")


class TestScanPath:
    def test_data_files_take_the_native_parquet_scan(self, surface):
        """s3:// data files under an Iceberg table must be read by the native
        parquet scan, the same as any other parquet - not a Python fallback."""
        session = opteryx.session(user="tests")
        rows(session, f"SELECT id, amount FROM {surface['main']}.events WHERE kind = 'a'")
        assert set(session.telemetry["scan_sources"].values()) == {"NativeParquetScanSource"}
