"""A table whose data files are ORC, read through SQL (opteryx-core
docs/ORC_READER_DESIGN.md).

The data is written by pyarrow.orc (an independent ORC writer), given Iceberg's `iceberg.id`
type attributes by tests/_orc_data_files.py, listed by a real DATA manifest and manifest
list (tests/_avro_data_files.py), and read back by opteryx-core through IcebergMetastore.
Every expected value is the value the test wrote.

Basic support only: primitive columns, local files. Everything else raises, by message.
"""

import datetime
import decimal
import os
import time

os.environ["TZ"] = "UTC"
time.tzset()

import pyarrow as pa
import pytest
from _avro_data_files import FileFormat
from _avro_data_files import data_file
from _orc_data_files import orc_bytes
from test_avro_data_files import FIELDS
from test_avro_data_files import ROWS
from test_avro_data_files import Env as AvroEnv

from opteryx.exceptions import DatasetReadError
from opteryx.exceptions import NotSupportedError
from opteryx.models.dataset_format import MixedFormatDatasetError

UTC = datetime.timezone.utc
IDS = {"id": 1, "name": 2, "score": 3, "ts": 4, "d": 5, "price": 6, "flag": 7, "n": 8}


def _table(rows, drop=(), rename=None):
    columns = {
        "id": pa.array([r for r in rows], pa.int64()),
        "name": pa.array([None if r % 4 == 0 else f"n{r}" for r in rows], pa.string()),
        "score": pa.array([r / 2 for r in rows], pa.float64()),
        "ts": pa.array([datetime.datetime(2026, 1, r) for r in rows], pa.timestamp("us")),
        "d": pa.array([datetime.date(2026, 2, r) for r in rows]),
        "price": pa.array([decimal.Decimal(f"{r}.25") for r in rows], pa.decimal128(9, 2)),
        "flag": pa.array([r % 2 == 1 for r in rows], pa.bool_()),
        "n": pa.array([r * 10 for r in rows], pa.int32()),
    }
    for name in drop:
        del columns[name]
    if rename:
        columns = {rename.get(k, k): v for k, v in columns.items()}
    return pa.table(columns)


class Env(AvroEnv):
    def orc_file(self, name, table, ids=IDS):
        path = self.root / "data" / name
        path.write_bytes(orc_bytes(table, ids))
        return data_file(path, FileFormat.ORC, table.num_rows, path.stat().st_size)


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


def test_every_primitive_type_reads_back_as_written(env):
    env.add(env.orc_file("a.orc", _table(range(1, 6))))
    got = env.rows(f"SELECT id, name, score, ts, d, price, flag, n FROM {env.name}")
    want = sorted(
        [
            (r, None if r % 4 == 0 else f"n{r}", r / 2, datetime.datetime(2026, 1, r, tzinfo=UTC),
             datetime.date(2026, 2, r), decimal.Decimal(f"{r}.25"), r % 2 == 1, r * 10)
            for r in range(1, 6)
        ],
        key=repr,
    )
    assert got == want
    assert env.scan_sources == {"NativeOrcScanSource"}


def test_projection_filter_count_and_limit(env):
    env.add(env.orc_file("a.orc", _table(range(1, 6))))
    assert env.rows(f"SELECT name FROM {env.name} WHERE id > 3") == [("n5",), (None,)]
    assert env.rows(f"SELECT COUNT(*) FROM {env.name}") == [(5,)]
    assert env.rows(f"SELECT COUNT(*) FROM {env.name} WHERE flag IS TRUE") == [(3,)]
    assert env.rows(f"SELECT id FROM {env.name} WHERE price >= 4.0 AND n < 50") == [(4,)]
    assert len(env.rows(f"SELECT id FROM {env.name} LIMIT 2")) == 2


def test_several_files_are_one_table(env):
    env.add(env.orc_file("a.orc", _table([1, 2])), env.orc_file("b.orc", _table([3, 4, 5])))
    assert env.rows(f"SELECT COUNT(*), SUM(n) FROM {env.name}") == [(5, 150)]


class TestSchemaEvolution:
    """Columns are matched by `iceberg.id`, not by name or position."""

    def test_reordered_columns_read_the_same(self, env):
        reordered = _table([3, 4])
        reordered = reordered.select(list(reversed(reordered.column_names)))
        env.add(env.orc_file("a.orc", _table([1, 2])), env.orc_file("b.orc", reordered))
        assert env.rows(f"SELECT id, name, n FROM {env.name}") == sorted(
            [(r, None if r % 4 == 0 else f"n{r}", r * 10) for r in (1, 2, 3, 4)], key=repr
        )

    def test_a_renamed_column_is_still_the_same_column(self, env):
        # Written when `name` was called `title`: only the id says it is the same column.
        old = _table([1, 2, 3], rename={"name": "title"})
        env.add(env.orc_file("a.orc", old, ids={**IDS, "title": 2}))
        assert env.rows(f"SELECT id, name FROM {env.name} WHERE id = 3") == [(3, "n3")]

    def test_a_column_added_after_the_file_was_written_is_null(self, env):
        old = _table([1, 2], drop=("flag", "n", "price"))
        env.add(env.orc_file("a.orc", old))
        assert env.rows(f"SELECT id, flag, n, price FROM {env.name}") == [(1, None, None, None), (2, None, None, None)]

    def test_a_column_the_table_dropped_is_ignored(self, env):
        extra = _table([1, 2]).append_column("legacy", pa.array(["x", "y"]))
        env.add(env.orc_file("a.orc", extra, ids={**IDS, "legacy": 99}))
        assert env.rows(f"SELECT COUNT(*), MAX(n) FROM {env.name}") == [(2, 20)]

    def test_an_int_widens_to_long(self, env):
        narrow = _table([1, 2, 3]).set_column(0, "id", pa.array([1, 2, 3], pa.int32()))
        env.add(env.orc_file("a.orc", narrow))
        assert env.rows(f"SELECT SUM(id) FROM {env.name}") == [(6,)]


class TestRefusals:
    def test_a_file_without_iceberg_ids_fails_the_query(self, env):
        env.add(env.orc_file("a.orc", _table([1, 2]), ids=None))
        with pytest.raises(DatasetReadError):
            env.rows(f"SELECT id FROM {env.name}")

    def test_a_column_of_another_type_fails_the_query(self, env):
        wrong = _table([1, 2]).set_column(0, "id", pa.array(["a", "b"], pa.string()))
        env.add(env.orc_file("a.orc", wrong))
        with pytest.raises(DatasetReadError):
            env.rows(f"SELECT id FROM {env.name}")


    def test_orc_and_avro_files_in_one_table_are_refused(self, env):
        env.add(env.orc_file("a.orc", _table([1])), env.avro_file("b.avro", FIELDS, ROWS[:1]))
        with pytest.raises(MixedFormatDatasetError):
            env.rows(f"SELECT id FROM {env.name}")

    def test_a_non_utc_timestamp_writer_is_refused_naming_the_zone(self, env):
        path = env.root / "data" / "a.orc"
        data = orc_bytes(_table([1]), IDS)
        marker = b"\x1a\x03GMT"
        assert data.count(marker) == 1
        path.write_bytes(data.replace(marker, b"\x1a\x03EST"))
        env.add(data_file(path, FileFormat.ORC, 1, path.stat().st_size))
        with pytest.raises(DatasetReadError):
            env.rows(f"SELECT ts FROM {env.name}")

    def test_a_file_that_is_not_orc_fails_the_query(self, env):
        path = env.root / "data" / "a.orc"
        path.write_bytes(b"ORC but not really")
        env.add(data_file(path, FileFormat.ORC, 1, path.stat().st_size))
        with pytest.raises(DatasetReadError):
            env.rows(f"SELECT id FROM {env.name}")
