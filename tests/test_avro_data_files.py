"""A table whose data files are Avro, read through SQL (opteryx-core
docs/ICEBERG_AVRO_ORC_DATA_FILES_DESIGN.md).

The data files are written with fastavro in the shape Iceberg's Avro writer produces
(tests/_avro_data_files.py), listed by a real DATA manifest and manifest list, and read
back by opteryx-core through IcebergMetastore. Every expected value is the value the
test wrote.

Basic support only: primitive columns, local files. Everything else is asserted to
raise, by message.
"""

import datetime
import decimal
import itertools

import pyarrow as pa
import pytest
from _avro_data_files import FileFormat
from _avro_data_files import add_files
from _avro_data_files import avro_field
from _avro_data_files import data_file
from _avro_data_files import write_avro_data_file
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import BooleanType
from pyiceberg.types import DateType
from pyiceberg.types import DecimalType
from pyiceberg.types import DoubleType
from pyiceberg.types import IntegerType
from pyiceberg.types import LongType
from pyiceberg.types import NestedField
from pyiceberg.types import StringType
from pyiceberg.types import TimestampType

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector
from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import NotSupportedError
from opteryx.models.dataset_format import MixedFormatDatasetError

from opteryx_iceberg import IcebergMetastore

UTC = datetime.timezone.utc
_workspaces = itertools.count()

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "name", StringType(), required=False),
    NestedField(3, "score", DoubleType(), required=False),
    NestedField(4, "ts", TimestampType(), required=False),
    NestedField(5, "d", DateType(), required=False),
    NestedField(6, "price", DecimalType(9, 2), required=False),
    NestedField(7, "flag", BooleanType(), required=False),
    NestedField(8, "n", IntegerType(), required=False),
)

TS = {"type": "long", "logicalType": "timestamp-micros", "adjust-to-utc": False}
DATE = {"type": "int", "logicalType": "date"}
PRICE = {
    "type": "fixed",
    "name": "decimal_9_2",
    "size": 4,
    "logicalType": "decimal",
    "precision": 9,
    "scale": 2,
}

FIELDS = [
    avro_field("id", 1, "long"),
    avro_field("name", 2, "string"),
    avro_field("score", 3, "double"),
    avro_field("ts", 4, TS),
    avro_field("d", 5, DATE),
    avro_field("price", 6, PRICE),
    avro_field("flag", 7, "boolean"),
    avro_field("n", 8, "int"),
]

ROWS = [
    {
        "id": i,
        "name": name,
        "score": score,
        "ts": datetime.datetime(2026, 1, i, 12, 30, tzinfo=UTC),
        "d": datetime.date(2026, 2, i),
        "price": decimal.Decimal(f"{i}.25"),
        "flag": i % 2 == 1,
        "n": i * 10,
    }
    for i, name, score in [
        (1, "ada", 9.5),
        (2, "grace", 8.25),
        (3, "alan", 7.0),
        (4, "edsger", 6.5),
        (5, "barbara", 1.5),
    ]
]


class Env:
    def __init__(self, tmp_path, schema=SCHEMA):
        self.root = tmp_path
        self.workspace = f"avro{next(_workspaces)}"
        (tmp_path / "warehouse").mkdir()
        (tmp_path / "data").mkdir()
        uri = f"sqlite:///{tmp_path / 'catalog.db'}"
        warehouse = f"file://{tmp_path / 'warehouse'}"
        self.catalog = SqlCatalog(self.workspace, **{"uri": uri, "warehouse": warehouse})
        self.catalog.create_namespace("ns")
        self.table = self.catalog.create_table("ns.t", schema=schema)
        register_workspace(
            self.workspace,
            OpteryxConnector,
            catalog=IcebergMetastore,
            catalog_type="sql",
            uri=uri,
            warehouse=warehouse,
        )
        self.name = f"{self.workspace}.ns.t"

    def avro_file(self, name, fields, records):
        path = self.root / "data" / name
        size = write_avro_data_file(path, fields, records)
        return data_file(path, FileFormat.AVRO, len(records), size)

    def add(self, *files):
        self.table = self.catalog.load_table("ns.t")
        add_files(self.table, list(files))

    def rows(self, sql):
        session = opteryx.session(user="tests")
        out = []
        for morsel in session.execute_to_morsels(sql):
            columns = [morsel.column(n).to_pylist() for n in morsel.column_names]
            out.extend(zip(*columns))
        self.scan_sources = set(session.telemetry["scan_sources"].values())
        return sorted(out, key=repr)


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def test_every_primitive_type_reads_back_as_written(env):
    env.add(env.avro_file("a.avro", FIELDS, ROWS))
    got = env.rows(f"SELECT id, name, score, ts, d, price, flag, n FROM {env.name}")
    want = sorted(
        [
            (r["id"], r["name"], r["score"], r["ts"], r["d"], r["price"], r["flag"], r["n"])
            for r in ROWS
        ],
        key=repr,
    )
    assert got == want
    assert env.scan_sources == {"NativeAvroScanSource"}


def test_projection_filter_and_count(env):
    env.add(env.avro_file("a.avro", FIELDS, ROWS))
    assert env.rows(f"SELECT name FROM {env.name} WHERE id > 3") == [("barbara",), ("edsger",)]
    assert env.rows(f"SELECT COUNT(*) FROM {env.name}") == [(5,)]
    assert env.rows(f"SELECT COUNT(*) FROM {env.name} WHERE flag IS TRUE") == [(3,)]
    assert env.rows(f"SELECT id FROM {env.name} WHERE price >= 4.0 AND n < 50") == [(4,)]
    assert env.rows(f"SELECT id FROM {env.name} WHERE name = 'alan'") == [(3,)]


def test_a_limit_is_applied_above_the_scan(env):
    env.add(env.avro_file("a.avro", FIELDS, ROWS))
    assert len(env.rows(f"SELECT id FROM {env.name} LIMIT 2")) == 2
    assert len(env.rows(f"SELECT id FROM {env.name} WHERE id > 1 LIMIT 3")) == 3


def test_null_values_survive(env):
    rows = [dict(ROWS[0], name=None, score=None, price=None), dict(ROWS[1])]
    env.add(env.avro_file("a.avro", FIELDS, rows))
    assert env.rows(f"SELECT id, name, score, price FROM {env.name} WHERE name IS NULL") == [
        (1, None, None, None)
    ]
    assert env.rows(f"SELECT COUNT(*) FROM {env.name} WHERE score IS NOT NULL") == [(1,)]


def test_several_files_are_one_table(env):
    env.add(
        env.avro_file("a.avro", FIELDS, ROWS[:2]),
        env.avro_file("b.avro", FIELDS, ROWS[2:]),
    )
    assert env.rows(f"SELECT COUNT(*), SUM(n) FROM {env.name}") == [(5, 150)]


class TestSchemaEvolution:
    """Fields are matched by field id, not by name or position."""

    def test_a_file_with_reordered_fields_reads_the_same(self, env):
        reordered = list(reversed(FIELDS))
        env.add(
            env.avro_file("a.avro", FIELDS, ROWS[:2]),
            env.avro_file("b.avro", reordered, ROWS[2:]),
        )
        assert env.rows(f"SELECT id, name, n FROM {env.name}") == sorted(
            [(r["id"], r["name"], r["n"]) for r in ROWS], key=repr
        )

    def test_a_renamed_column_is_still_the_same_column(self, env):
        # The file was written when `name` was called `title`: only the id says it
        # is the same column once the table renames it.
        old = [dict(f, name="title") if f["field-id"] == 2 else f for f in FIELDS]
        old_rows = [{("title" if k == "name" else k): v for k, v in r.items()} for r in ROWS]
        env.add(env.avro_file("a.avro", old, old_rows))
        assert env.rows(f"SELECT id, name FROM {env.name} WHERE id = 2") == [(2, "grace")]

    def test_a_column_added_after_the_file_was_written_is_null(self, env):
        env.add(env.avro_file("a.avro", FIELDS[:3], [
            {"id": r["id"], "name": r["name"], "score": r["score"]} for r in ROWS
        ]))
        got = env.rows(f"SELECT id, flag, n, price FROM {env.name} WHERE id <= 2")
        assert got == [(1, None, None, None), (2, None, None, None)]

    def test_a_dropped_field_in_a_file_is_ignored(self, env):
        extra = FIELDS + [avro_field("legacy", 99, "string")]
        rows = [dict(r, legacy="x") for r in ROWS]
        env.add(env.avro_file("a.avro", extra, rows))
        assert env.rows(f"SELECT COUNT(*), MAX(n) FROM {env.name}") == [(5, 50)]

    def test_an_int_promotes_to_long(self, env):
        fields = [avro_field("id", 1, "int")] + FIELDS[1:]
        env.add(env.avro_file("a.avro", fields, ROWS))
        assert env.rows(f"SELECT SUM(id) FROM {env.name}") == [(15,)]


class TestRefusals:
    def test_a_file_without_field_ids_fails_the_query(self, env):
        bare = [{k: v for k, v in f.items() if k != "field-id"} for f in FIELDS]
        env.add(env.avro_file("a.avro", bare, ROWS))
        with pytest.raises(DatasetReadError):
            env.rows(f"SELECT id FROM {env.name}")

    def test_a_field_of_another_type_fails_the_query(self, env):
        fields = [avro_field("id", 1, "string")] + FIELDS[1:]
        env.add(env.avro_file("a.avro", fields, [dict(r, id="x") for r in ROWS]))
        with pytest.raises(DatasetReadError):
            env.rows(f"SELECT id FROM {env.name}")


    def test_avro_and_parquet_files_in_one_table_are_refused(self, env):
        table = pa.table({"id": pa.array([9], pa.int64())})
        parquet = env.root / "data" / "p.parquet"
        import pyarrow.parquet as pq

        pq.write_table(table, parquet)
        env.add(
            env.avro_file("a.avro", FIELDS, ROWS),
            data_file(parquet, FileFormat.PARQUET, 1, parquet.stat().st_size),
        )
        with pytest.raises(MixedFormatDatasetError):
            env.rows(f"SELECT id FROM {env.name}")



def test_an_avro_table_still_prunes_by_nothing_it_does_not_have(env):
    """Iceberg's Avro writer records no column bounds: the manifest carries none, and
    a predicate must read the file rather than prune it."""
    env.add(env.avro_file("a.avro", FIELDS, ROWS))
    assert env.rows(f"SELECT COUNT(*) FROM {env.name} WHERE id = 3") == [(1,)]
    assert env.rows(f"SELECT COUNT(*) FROM {env.name} WHERE id = 999") == [(0,)]
