"""Iceberg uuid, time and fixed columns, read through SQL
(opteryx-core docs/UUID_AND_NESTED_COLUMNS_DESIGN.md, UD1-UD8).

uuid binds as the engine's UUID type and fixed(L) as VARBINARY - in Parquet (uuid), Avro (uuid; fixed
is refused, loudly) and ORC (both, driven by the `iceberg.binary-type` / `iceberg.length`
attributes). `time` follows Parquet, which is canon: INT64 microseconds since midnight in all three formats.
Every expected value is the value the test wrote."""

import datetime
import decimal
import uuid

import pyarrow as pa
import pytest
from _avro_data_files import FileFormat
from _avro_data_files import avro_field
from _avro_data_files import data_file
from _avro_data_files import write_avro_data_file
from _orc_data_files import orc_bytes
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import DecimalType
from pyiceberg.types import FixedType
from pyiceberg.types import LongType
from pyiceberg.types import NestedField
from pyiceberg.types import TimeType
from pyiceberg.types import UUIDType
from test_avro_data_files import Env

from opteryx.exceptions import DatasetReadError

UUIDS = [
    uuid.UUID("550e8400-e29b-41d4-a716-446655440000"),
    uuid.UUID("00000000-0000-0000-0000-000000000001"),
    uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
]
FIXED = [b"abcd", b"\x00\x01\x02\x03", b"wxyz"]

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(2, "u", UUIDType(), required=False),
    NestedField(3, "d1", DecimalType(9, 2), required=False),
    NestedField(4, "d2", DecimalType(9, 2), required=False),
)
WITH_FIXED = Schema(*SCHEMA.fields, NestedField(5, "f", FixedType(4), required=False))


def _sort(rows):
    return sorted(rows, key=repr)


# ── Parquet (written by pyiceberg itself) ───────────────────────────────────


@pytest.fixture
def parquet_table(tmp_path):
    env = Env(tmp_path, schema=Schema(*SCHEMA.fields[:2]))
    table = env.catalog.load_table("ns.t")
    table.append(
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4], pa.int64()),
                "u": pa.array([u.bytes for u in UUIDS] + [None], pa.binary(16)),
            }
        )
    )
    return env


def test_parquet_uuid_reads_compares_orders_and_aggregates(parquet_table):
    env = parquet_table
    assert env.rows(f"SELECT id, u FROM {env.name}") == _sort([(1, UUIDS[0]), (2, UUIDS[1]), (3, UUIDS[2]), (4, None)])
    assert env.rows(f"SELECT id FROM {env.name} WHERE u = '550e8400-e29b-41d4-a716-446655440000'") == [(1,)]
    assert env.rows(f"SELECT MAX(u), MIN(u), COUNT(u) FROM {env.name}") == [(UUIDS[2], UUIDS[1], 3)]
    assert env.rows(f"SELECT COUNT(*) FROM {env.name} WHERE u IS NULL") == [(1,)]
    assert env.rows(f"SELECT u, COUNT(*) FROM {env.name} WHERE u IS NOT NULL GROUP BY u") == _sort([(u, 1) for u in UUIDS])


TIMES = [datetime.time(0, 0, 1), datetime.time(12, 30, 45, 123456), datetime.time(23, 59, 59, 999999)]
# Parquet is canon: the engine reads a TIME column as INT64 microseconds since midnight, and Avro and ORC
# fold into that - so the same `time` value is the same integer whatever the data file's format.
MICROS = [((t.hour * 60 + t.minute) * 60 + t.second) * 1_000_000 + t.microsecond for t in TIMES]
TIME_SCHEMA = Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "t", TimeType(), required=False))


def _time_expected():
    return _sort([(i + 1, MICROS[i]) for i in range(3)])


def test_parquet_time_is_int64_microseconds(tmp_path):
    env = Env(tmp_path, schema=TIME_SCHEMA)
    env.catalog.load_table("ns.t").append(
        pa.table({"id": pa.array([1, 2, 3], pa.int64()), "t": pa.array(TIMES, pa.time64("us"))})
    )
    assert env.rows(f"SELECT id, t FROM {env.name}") == _time_expected()
    assert env.rows(f"SELECT MAX(t), MIN(t) FROM {env.name}") == [(MICROS[2], MICROS[0])]


def test_avro_time_is_the_same_integer(tmp_path):
    env = Env(tmp_path, schema=TIME_SCHEMA)
    path = tmp_path / "data" / "a.avro"
    fields = [avro_field("id", 1, "long"), avro_field("t", 2, {"type": "long", "logicalType": "time-micros"})]
    size = write_avro_data_file(path, fields, [{"id": i + 1, "t": TIMES[i]} for i in range(3)])
    env.add(data_file(path, FileFormat.AVRO, 3, size))
    assert env.rows(f"SELECT id, t FROM {env.name}") == _time_expected()


def test_orc_time_is_the_same_integer(tmp_path):
    env = Env(tmp_path, schema=TIME_SCHEMA)
    path = tmp_path / "data" / "a.orc"
    table = pa.table({"id": pa.array([1, 2, 3], pa.int64()), "t": pa.array(MICROS, pa.int64())})
    path.write_bytes(orc_bytes(table, {"id": 1, "t": 2}, {"t": {"iceberg.long-type": "TIME"}}))
    env.add(data_file(path, FileFormat.ORC, 3, path.stat().st_size))
    assert env.rows(f"SELECT id, t FROM {env.name}") == _time_expected()


def test_parquet_fixed_is_refused_loudly(tmp_path):
    env = Env(tmp_path, schema=Schema(NestedField(1, "id", LongType(), required=False), NestedField(2, "f", FixedType(4), required=False)))
    env.catalog.load_table("ns.t").append(pa.table({"id": pa.array([1], pa.int64()), "f": pa.array([b"abcd"], pa.binary(4))}))
    with pytest.raises(Exception, match="fixed_len_byte_array|fixed"):
        env.rows(f"SELECT f FROM {env.name}")


# ── Avro ────────────────────────────────────────────────────────────────────


def _avro_fields(with_fixed=False):
    fields = [
        avro_field("id", 1, "long"),
        avro_field("u", 2, {"type": "fixed", "name": "u_fixed", "size": 16, "logicalType": "uuid"}),
        # two decimals of the SAME shape: each is a named type, defined once per schema
        avro_field("d1", 3, {"type": "fixed", "name": "d1_dec", "size": 4, "logicalType": "decimal", "precision": 9, "scale": 2}),
        avro_field("d2", 4, {"type": "fixed", "name": "d2_dec", "size": 4, "logicalType": "decimal", "precision": 9, "scale": 2}),
    ]
    if with_fixed:
        fields.append(avro_field("f", 5, {"type": "fixed", "name": "f_fixed", "size": 4}))
    return fields


def _avro_rows(with_fixed=False):
    rows = [
        {"id": i + 1, "u": UUIDS[i].bytes, "d1": decimal.Decimal(f"{i}.25"), "d2": decimal.Decimal(f"{i}.50")}
        for i in range(3)
    ]
    if with_fixed:
        for i, row in enumerate(rows):
            row["f"] = FIXED[i]
    return rows


def test_avro_uuid_and_two_decimals_of_one_shape(tmp_path):
    env = Env(tmp_path, schema=SCHEMA)
    path = tmp_path / "data" / "a.avro"
    size = write_avro_data_file(path, _avro_fields(), _avro_rows())
    env.add(data_file(path, FileFormat.AVRO, 3, size))
    assert env.rows(f"SELECT id, u, d1, d2 FROM {env.name}") == _sort(
        [(i + 1, UUIDS[i], decimal.Decimal(f"{i}.25"), decimal.Decimal(f"{i}.50")) for i in range(3)]
    )
    assert env.rows(f"SELECT id FROM {env.name} WHERE u = '00000000-0000-0000-0000-000000000001'") == [(2,)]


def test_avro_fixed_reads_as_varbinary(tmp_path):
    # An Iceberg `fixed(L)` binds as VARBINARY, which the table reader schema can only express as
    # `bytes`; rugo reads the file's fixed as those bytes (the one non-spec promotion, avro_reader.cpp).
    env = Env(tmp_path, schema=WITH_FIXED)
    path = tmp_path / "data" / "a.avro"
    size = write_avro_data_file(path, _avro_fields(with_fixed=True), _avro_rows(with_fixed=True))
    env.add(data_file(path, FileFormat.AVRO, 3, size))
    assert env.rows(f"SELECT id, f FROM {env.name}") == _sort([(i + 1, FIXED[i]) for i in range(3)])


# ── ORC (type attributes written into the footer) ───────────────────────────


def _orc_table(uuid_bytes=None):
    return pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "u": pa.array(uuid_bytes or [u.bytes for u in UUIDS], pa.binary()),
            "f": pa.array(FIXED, pa.binary()),
        }
    )


IDS = {"id": 1, "u": 2, "f": 5}
ATTRIBUTES = {
    "u": {"iceberg.binary-type": "UUID"},
    "f": {"iceberg.binary-type": "FIXED", "iceberg.length": "4"},
}


def _orc_env(tmp_path, table):
    env = Env(tmp_path, schema=WITH_FIXED)
    path = tmp_path / "data" / "a.orc"
    path.write_bytes(orc_bytes(table, IDS, ATTRIBUTES))
    env.add(data_file(path, FileFormat.ORC, table.num_rows, path.stat().st_size))
    return env


def test_orc_uuid_and_fixed(tmp_path):
    env = _orc_env(tmp_path, _orc_table())
    assert env.rows(f"SELECT id, u, f FROM {env.name}") == _sort([(i + 1, UUIDS[i], FIXED[i]) for i in range(3)])
    assert env.rows(f"SELECT id FROM {env.name} WHERE u = '550e8400-e29b-41d4-a716-446655440000'") == [(1,)]
    assert env.rows(f"SELECT MAX(u), MIN(u) FROM {env.name}") == [(UUIDS[2], UUIDS[1])]


def test_orc_a_uuid_value_that_is_not_16_bytes_fails_naming_the_column(tmp_path):
    env = _orc_env(tmp_path, _orc_table(uuid_bytes=[UUIDS[0].bytes, b"short", UUIDS[2].bytes]))
    with pytest.raises(DatasetReadError):
        env.rows(f"SELECT u FROM {env.name}")
    assert env.rows(f"SELECT COUNT(*) FROM {env.name}") == [(3,)]
