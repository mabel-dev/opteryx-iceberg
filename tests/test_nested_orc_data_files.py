"""Iceberg STRUCT / MAP / LIST columns in ORC data files
(opteryx-core docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.1 and §3.3, option (b)).

The same contract as the Parquet tests (tests/test_nested_columns.py): a struct, a string-keyed map and
a list with a struct at the bottom are one JSON value per row, a list of a plain scalar is an ARRAY, and
each file's nested field ids and struct member names are verified against the table's declared shape -
a file that disagrees (the table was evolved after it was written) fails the query naming the field.

pyarrow.orc writes the data; tests/_orc_data_files.py gives every nested type Iceberg's `iceberg.id`
attribute. Every expected value is the value the test wrote.
"""

import json
import os
import time

os.environ["TZ"] = "UTC"
time.tzset()

import pyarrow as pa
import pytest
from _avro_data_files import FileFormat
from _avro_data_files import data_file
from _orc_data_files import orc_bytes
from _orc_data_files import with_nested_iceberg_ids
from opteryx.exceptions import DatasetReadError
from pyiceberg.schema import Schema
from pyiceberg.types import ListType
from pyiceberg.types import LongType
from pyiceberg.types import MapType
from pyiceberg.types import NestedField
from pyiceberg.types import StringType
from pyiceberg.types import StructType
from test_avro_data_files import Env as AvroEnv

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(
        2,
        "s",
        StructType(NestedField(10, "a", LongType(), required=False), NestedField(11, "b", StringType(), required=False)),
        required=False,
    ),
    NestedField(3, "m", MapType(12, StringType(), 13, LongType(), value_required=False), required=False),
    NestedField(4, "ls", ListType(14, LongType(), element_required=False), required=False),
    NestedField(
        5,
        "lst",
        ListType(15, StructType(NestedField(16, "k", LongType(), required=False)), element_required=False),
        required=False,
    ),
)


def _arrow(a_name="a"):
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("s", pa.struct([pa.field(a_name, pa.int64()), pa.field("b", pa.string())])),
            pa.field("m", pa.map_(pa.string(), pa.int64())),
            pa.field("ls", pa.list_(pa.int64())),
            pa.field("lst", pa.list_(pa.struct([pa.field("k", pa.int64())]))),
        ]
    )


def _rows(first_id, a_name="a"):
    return pa.table(
        {
            "id": [first_id, first_id + 1],
            "s": [{a_name: first_id, "b": "x"}, None],
            "m": [[("k", first_id)], None],
            "ls": [[1, 2], None],
            "lst": [[{"k": first_id}], []],
        },
        schema=_arrow(a_name),
    )


class Env(AvroEnv):
    def ids(self, renamed=None):
        """{path: field id} for every type of the table's CURRENT schema, as Iceberg's ORC writer sets them
        (`renamed` maps a path to the name it had when the file was written)."""
        schema = self.catalog.load_table("ns.t").schema()
        out = {}

        def walk(path, field_type, field_id):
            out[path] = field_id
            if isinstance(field_type, StructType):
                for f in field_type.fields:
                    walk(f"{path}.{f.name}", f.field_type, f.field_id)
            elif isinstance(field_type, ListType):
                walk(f"{path}[]", field_type.element_type, field_type.element_id)
            elif isinstance(field_type, MapType):
                walk(f"{path}.key", field_type.key_type, field_type.key_id)
                walk(f"{path}.value", field_type.value_type, field_type.value_id)

        for f in schema.fields:
            walk(f.name, f.field_type, f.field_id)
        for old_path, new_path in (renamed or {}).items():
            out[old_path] = out.pop(new_path)
        return out

    def orc_file(self, name, table, renamed=None):
        path = self.root / "data" / name
        path.write_bytes(with_nested_iceberg_ids(orc_bytes(table), self.ids(renamed)))
        return data_file(path, FileFormat.ORC, table.num_rows, path.stat().st_size)


@pytest.fixture
def env(tmp_path):
    env = Env(tmp_path, schema=SCHEMA)
    env.add(env.orc_file("a.orc", _rows(1)))
    return env


def _json(env, column):
    out = {}
    for row_id, value in env.rows(f"SELECT id, {column} FROM {env.name}"):
        out[row_id] = None if value is None else (json.loads(value) if isinstance(value, str) else value)
    return out


def test_an_unevolved_orc_table_reads_every_nested_shape(env):
    assert _json(env, "s") == {1: {"a": 1, "b": "x"}, 2: None}
    assert _json(env, "m") == {1: {"k": 1}, 2: None}
    assert _json(env, "ls") == {1: [1, 2], 2: None}
    assert _json(env, "lst") == {1: [{"k": 1}], 2: []}
    assert env.scan_sources == {"NativeOrcScanSource"}


def test_a_renamed_nested_field_fails_naming_it(env, caplog):
    with env.catalog.load_table("ns.t").update_schema() as update:
        update.rename_column("s.a", "aa")
    env.table = env.catalog.load_table("ns.t")
    env.add(env.orc_file("b.orc", _rows(10, "aa")))     # written after the rename; a.orc was not
    # An ORC file is opened at execution time, where the engine reports a read failure to the caller
    # generically and logs the cause - which names the field and the file.
    with pytest.raises(DatasetReadError):
        env.rows(f"SELECT id, s FROM {env.name}")
    assert "column 's.a': the table declares this field as 'aa' (id" in caplog.text
    assert "a.orc" in caplog.text
    assert _json(env, "ls") == {1: [1, 2], 2: None, 10: [1, 2], 11: None}   # columns not touched are fine


def test_a_file_without_the_nested_field_ids_fails_naming_the_column(env, caplog):
    path = env.root / "data" / "bare.orc"
    top_level = {k: v for k, v in env.ids().items() if "." not in k and "[" not in k}
    path.write_bytes(with_nested_iceberg_ids(orc_bytes(_rows(20)), top_level))     # ids on the columns, none on their nested types
    env.add(data_file(path, FileFormat.ORC, 2, path.stat().st_size))
    with pytest.raises(DatasetReadError):
        env.rows(f"SELECT id, s FROM {env.name}")
    assert "carries no iceberg.id for it" in caplog.text
