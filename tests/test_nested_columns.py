"""Iceberg STRUCT / MAP / LIST columns, read through SQL
(opteryx-core docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.1 and §3.3, options (b) and (c)).

A struct, a map with string keys, and a list with a struct or map at the bottom are one JSON value per
row; a list of a plain scalar is an ARRAY. A JSON object is made of NAMES and Iceberg identifies a
nested field by ID, so each data file's nested fields are MATCHED BY ID to the table's declared nested shape and rendered with
the table's names (option (c)): a file written before a rename, an add or a drop reads as the table says -
a renamed member under its new name, an added member as null, a dropped member not at all.

pyiceberg writes the tables and data (Parquet); every expected value is the value the test wrote.
"""

import json

import pyarrow as pa
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType
from pyiceberg.types import ListType
from pyiceberg.types import LongType
from pyiceberg.types import MapType
from pyiceberg.types import NestedField
from pyiceberg.types import StringType
from pyiceberg.types import StructType
from test_avro_data_files import Env

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=False),
    NestedField(
        2,
        "s",
        StructType(
            NestedField(10, "a", LongType(), required=False),
            NestedField(11, "b", StringType(), required=False),
        ),
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

ARROW = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("s", pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.string())])),
        pa.field("m", pa.map_(pa.string(), pa.int64())),
        pa.field("ls", pa.list_(pa.field("element", pa.int64()))),
        pa.field("lst", pa.list_(pa.field("element", pa.struct([pa.field("k", pa.int64())])))),
    ]
)
ROWS = {
    "id": [1, 2, 3],
    "s": [{"a": 1, "b": "x"}, None, {"a": None, "b": 'q"t'}],
    "m": [[("a", 1)], None, []],
    "ls": [[1, 2], None, []],
    "lst": [[{"k": 1}, {"k": 2}], None, []],
}


def _table():
    return pa.table(ROWS, schema=ARROW)


@pytest.fixture
def env(tmp_path):
    env = Env(tmp_path, schema=SCHEMA)
    env.catalog.load_table("ns.t").append(_table())
    return env


def _json_rows(env, column):
    out = {}
    for row_id, value in env.rows(f"SELECT id, {column} FROM {env.name}"):
        out[row_id] = None if value is None else (json.loads(value) if isinstance(value, str) else value)
    return out


def test_an_unevolved_table_reads_every_nested_shape(env):
    assert _json_rows(env, "s") == {1: {"a": 1, "b": "x"}, 2: None, 3: {"a": None, "b": 'q"t'}}
    assert _json_rows(env, "m") == {1: {"a": 1}, 2: None, 3: {}}
    assert _json_rows(env, "ls") == {1: [1, 2], 2: None, 3: []}      # a list of a scalar is an ARRAY
    # (the Parquet pyiceberg writes stores a NULL list<struct> as an empty list; the file says so)
    assert _json_rows(env, "lst") == {1: [{"k": 1}, {"k": 2}], 2: [], 3: []}


def test_a_nested_column_with_a_filter_and_other_columns(env):
    assert env.rows(f"SELECT id, s FROM {env.name} WHERE id = 1") == [(1, '{"a":1,"b":"x"}')]


def _append_after_evolution(env, struct_fields):
    """Append a second batch under the evolved schema (a new snapshot, written under the new schema id,
    so the table now holds a file from before the change and one from after)."""
    schema = pa.schema(
        [
            ARROW.field("id"),
            pa.field("s", pa.struct(struct_fields)),
            ARROW.field("m"),
            ARROW.field("ls"),
            ARROW.field("lst"),
        ]
    )
    values = {name: [None] for name in ("m", "ls", "lst")}
    values["id"] = [9]
    values["s"] = [{f.name: None for f in struct_fields}]
    env.catalog.load_table("ns.t").append(pa.table(values, schema=schema))


def test_a_renamed_nested_field_reads_under_the_new_name(env):
    with env.catalog.load_table("ns.t").update_schema() as update:
        update.rename_column("s.a", "aa")
    _append_after_evolution(env, [pa.field("aa", pa.int64()), pa.field("b", pa.string())])
    # The file written before the rename says "a"; the table says "aa" - same field id, so the old
    # rows render under the table's name and the new row alongside them.
    got = _json_rows(env, "s")
    assert got == {1: {"aa": 1, "b": "x"}, 2: None, 3: {"aa": None, "b": 'q"t'}, 9: {"aa": None, "b": None}}
    assert env.rows(f"SELECT COUNT(*) FROM {env.name}") == [(4,)]


def test_a_nested_field_added_after_a_file_was_written_is_null_in_the_old_file(env):
    with env.catalog.load_table("ns.t").update_schema() as update:
        update.add_column(("s", "c"), LongType())
    _append_after_evolution(
        env, [pa.field("a", pa.int64()), pa.field("b", pa.string()), pa.field("c", pa.int64())]
    )
    got = _json_rows(env, "s")
    assert got[1] == {"a": 1, "b": "x", "c": None}          # the old file has no "c"
    assert got[2] is None
    assert got[9] == {"a": None, "b": None, "c": None}      # the new file has it


def test_a_nested_field_dropped_after_a_file_was_written_is_not_rendered(env):
    with env.catalog.load_table("ns.t").update_schema() as update:
        update.delete_column(("s", "b"))
    _append_after_evolution(env, [pa.field("a", pa.int64())])
    got = _json_rows(env, "s")
    assert got[1] == {"a": 1}                               # the old file still holds "b"; the table dropped it
    assert got[3] == {"a": None}


def test_a_reordered_struct_renders_in_the_tables_order(env):
    with env.catalog.load_table("ns.t").update_schema() as update:
        update.move_first("s.b")
    _append_after_evolution(env, [pa.field("b", pa.string()), pa.field("a", pa.int64())])
    text = dict(env.rows(f"SELECT id, s FROM {env.name}"))
    assert text[1] == '{"b":"x","a":1}'


def test_a_list_of_structs_follows_a_renamed_member(env):
    with env.catalog.load_table("ns.t").update_schema() as update:
        update.rename_column("lst.element.k", "kk")
    schema = pa.schema(
        [
            ARROW.field("id"),
            ARROW.field("s"),
            ARROW.field("m"),
            ARROW.field("ls"),
            pa.field("lst", pa.list_(pa.field("element", pa.struct([pa.field("kk", pa.int64())])))),
        ]
    )
    env.catalog.load_table("ns.t").append(
        pa.table({"id": [9], "s": [None], "m": [None], "ls": [None], "lst": [[{"kk": 7}]]}, schema=schema)
    )
    got = _json_rows(env, "lst")
    assert got[1] == [{"kk": 1}, {"kk": 2}]
    assert got[9] == [{"kk": 7}]


def test_reading_the_old_snapshot_after_the_change_still_works(env):
    # The snapshot written before the rename is read through the schema it was written under.
    old = env.catalog.load_table("ns.t").current_snapshot().snapshot_id
    with env.catalog.load_table("ns.t").update_schema() as update:
        update.rename_column("s.a", "aa")
    _append_after_evolution(env, [pa.field("aa", pa.int64()), pa.field("b", pa.string())])
    rows = env.rows(f"SELECT id, s FROM {env.name} VERSION AS OF {old}")
    assert sorted(i for i, _ in rows) == [1, 2, 3]
    assert json.loads(dict(rows)[1]) == {"a": 1, "b": "x"}


def test_a_map_with_non_string_keys_is_refused_at_schema_time_naming_it(tmp_path):
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "m", MapType(3, IntegerType(), 4, LongType(), value_required=False), required=False),
    )
    env = Env(tmp_path, schema=schema)
    with pytest.raises(Exception, match="map with .*keys"):
        env.rows(f"SELECT id FROM {env.name}")


def test_a_nested_column_in_an_avro_data_file_is_refused_naming_it(tmp_path):
    # Iceberg nested columns are verified for Parquet and ORC data files; Avro data files carry the ids
    # but the table-side reader schema cannot express a nested column yet (design §6 step 4).
    from _avro_data_files import avro_field
    from opteryx.exceptions import NotSupportedError

    env = Env(tmp_path, schema=SCHEMA)
    env.add(env.avro_file("a.avro", [avro_field("id", 1, "long")], [{"id": 1}]))
    assert env.rows(f"SELECT id FROM {env.name}") == [(1,)]
    with pytest.raises(NotSupportedError, match="column 's'"):
        env.rows(f"SELECT id, s FROM {env.name}")
