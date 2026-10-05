"""End-to-end SQL over a purely local Iceberg catalog.

tests/test_sqlcatalog_read.py stops at the Metastore surface - load_dataset,
scan(), schema(). Nothing in this repo's suite has ever pushed a SQL statement
through opteryx-core against an Iceberg table, which is why the defects below
survived: everything opteryx-iceberg hands back LOOKS correct, and only an
engine actually reading it notices otherwise.

Some of these tests are EXPECTED TO FAIL. They are executable statements of what
the stack should do, not a description of what it does. Three defects, in the
state they were left in:

  1. FIXED. IcebergDataset.scan yielded data_file.file_path verbatim, so a
     local warehouse's "file:///..." path reached rugo's C++ parquet reader
     with the scheme still attached and it died with "RuntimeError: Cannot
     open file" (rugo/src/parquet/filesystem.hpp:137) - which blocked EVERY
     test in TestEndToEnd before any query semantics were exercised. scan now
     rewrites a local "file://" URI to the plain OS path the engine's local
     filesystem reads (see dataset._reader_path); "gs://"/"s3://" locations
     are passed through untouched.

  2. Predicates over the narrower numeric types returned the wrong row count -
     usually zero, and not even stably so between runs. Root cause was in THIS
     repo, not the engine: dataset.py declared Iceberg's 4-byte IntegerType as
     INTEGER (= INT64) and its 4-byte FloatType as DOUBLE (= FLOAT64), so the
     reader took 8 bytes per value from a 4-byte column and filtered garbage.
     Fixed by mapping each Iceberg type to the same-width Opteryx type. These
     tests are what caught it and what keeps it caught.

     Defect 1 still masks the whole class, so TestPredicatePruning re-runs the
     predicate cases with defect 1 explicitly neutralised (see the
     `strip_file_scheme` fixture) - that is there to keep any surviving
     wrong-answer failure visible and separately attributable, not to excuse it.

  3. FIXED (in the engine). DECIMAL predicates gave wrong answers on some
     operators: `d = 3.30` and `d < 8.0` returned 0 while `d > 1.0` and
     `d >= 3.30` were right - the shape of a comparison made against unscaled
     values. The cause was row-group pruning, not the decimal decode: rugo's
     statistics decoder (`decode_value`) returned an INT32/INT64 DECIMAL bound
     as its raw unscaled integer (1.10 as 110), so `3.30 < 110` discarded the
     row group. It only showed through a catalog because OpteryxConnector pushes
     DECIMAL predicates into the scan and DiskConnector declines them, so a plain
     parquet file of the same data never reached that comparison. The `d = 3.30`
     cases carried a strict xfail until the engine fix flipped them to XPASS.

Do not "fix" a failure here by relaxing an assertion: every expected value is
hand-computed from ROWS below and is arithmetic, not observed behaviour.
"""

from decimal import Decimal

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

from opteryx_iceberg import IcebergMetastore
from opteryx_iceberg.dataset import IcebergDataset

# The pyiceberg catalog NAME must equal the Opteryx workspace prefix:
# IcebergMetastore calls load_catalog(workspace, ...) and SqlCatalog stores that
# name in its own metadata table, so a mismatch surfaces as an unfindable table
# rather than as a configuration error.
WORKSPACE = "local"
TABLE = f"{WORKSPACE}.ns.t"

# One datafile, seven rows, one column per type under test.
ROWS = {
    "i64": pa.array([1, 2, 3, 4, 5, 6, 7], pa.int64()),
    "i32": pa.array([10, 20, 30, 40, 50, 60, 70], pa.int32()),
    "f64": pa.array([9.5, 8.25, 7.0, 6.5, 9.0, 1.5, 2.25], pa.float64()),
    "f32": pa.array([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7], pa.float32()),
    "s": pa.array(["ada", "grace", "alan", "edsger", "barbara", "katherine", "linus"]),
    "b": pa.array([True, False, True, False, True, False, True], pa.bool_()),
    "d": pa.array(
        [Decimal(v) for v in ("1.10", "2.20", "3.30", "4.40", "5.50", "6.60", "7.70")],
        pa.decimal128(9, 2),
    ),
}
ROW_COUNT = 7

# (predicate, expected row count, type under test). Every count is counted off
# ROWS by hand - see the comment on each line.
PREDICATES = [
    ("i64 > 3", 4, "INT64"),  # 4,5,6,7
    ("i64 = 2", 1, "INT64"),  # 2
    ("i32 >= 40", 4, "INT32"),  # 40,50,60,70
    ("i32 = 30", 1, "INT32"),  # 30
    ("f64 > 8", 3, "FLOAT64"),  # 9.5,8.25,9.0
    ("f64 = 7.0", 1, "FLOAT64"),  # 7.0
    ("f32 > 0.25", 5, "FLOAT32"),  # 0.3,0.4,0.5,0.6,0.7
    ("f32 < 0.25", 2, "FLOAT32"),  # 0.1,0.2
    ("s > 'b'", 5, "VARCHAR"),  # barbara,edsger,grace,katherine,linus
    ("s = 'alan'", 1, "VARCHAR"),  # alan
    ("b = true", 4, "BOOLEAN"),  # rows 1,3,5,7
    ("b = false", 3, "BOOLEAN"),  # rows 2,4,6
    ("d > 4.0", 4, "DECIMAL"),  # 4.40,5.50,6.60,7.70
    ("d = 3.30", 1, "DECIMAL"),  # 3.30
    # The bounds themselves. The literal is a float and the decoded bounds are
    # exact Decimals: 7.70 as a float is 7.7000000000000001776, ABOVE the max,
    # so a pruner comparing the raw float drops the row group holding it.
    ("d = 7.70", 1, "DECIMAL"),  # 7.70 - the max
    ("d >= 7.70", 1, "DECIMAL"),  # 7.70 - the max
    ("d = 1.10", 1, "DECIMAL"),  # 1.10 - the min
    ("d <= 1.10", 1, "DECIMAL"),  # 1.10 - the min
    ("d < 8.0", 7, "DECIMAL"),  # all seven
    ("d = 3.305", 0, "DECIMAL"),  # off the scale grid - matches nothing
]

PREDICATE_PARAMS = [
    pytest.param(predicate, expected, id=f"{kind}-{predicate}")
    for predicate, expected, kind in PREDICATES
]


@pytest.fixture(scope="module")
def session(tmp_path_factory):
    """A local SqlCatalog (SQLite metadata, local-disk FileIO) registered as an
    Opteryx workspace, and an Opteryx session pointed at it. No server, no cloud.
    """
    root = tmp_path_factory.mktemp("iceberg-e2e")
    warehouse = root / "warehouse"
    warehouse.mkdir()
    uri = f"sqlite:///{root / 'catalog.db'}"
    warehouse_uri = f"file://{warehouse}"

    table = pa.table(ROWS)
    writer = SqlCatalog(WORKSPACE, **{"uri": uri, "warehouse": warehouse_uri})
    writer.create_namespace("ns")
    writer.create_table("ns.t", schema=table.schema).append(table)

    register_workspace(
        WORKSPACE,
        OpteryxConnector,
        catalog=IcebergMetastore,
        catalog_type="sql",
        uri=uri,
        warehouse=warehouse_uri,
    )
    return opteryx.session(user="tests")


@pytest.fixture
def strip_file_scheme(monkeypatch):
    """Neutralise defect 1 (and ONLY defect 1) for the duration of one test.

    Strips any surviving "file://" scheme from the manifest entries
    IcebergDataset builds (`_manifest_entries`, behind both `scan` and
    `manifest_bytes`), so the query reaches the parquet reader and whatever it
    then gets wrong is a predicate-handling defect and nothing else. Now that the
    entries normalise the path themselves this is a no-op on a healthy tree - it
    is kept so that a regression in _reader_path shows up as a TestEndToEnd
    failure ONLY, leaving TestPredicatePruning's attribution ("not the file://
    scheme") true by construction rather than by assumption.
    """
    original = IcebergDataset._manifest_entries

    def entries(self, *args, **kwargs):
        for entry in original(self, *args, **kwargs):
            if entry["file_path"].startswith("file://"):
                entry["file_path"] = entry["file_path"][len("file://") :]
            yield entry

    monkeypatch.setattr(IcebergDataset, "_manifest_entries", entries)


def morsels(session, sql):
    return list(session.execute_to_morsels(sql))


def row_count(session, sql):
    return sum(morsel.num_rows for morsel in morsels(session, sql))


def column(session, sql, name):
    values = []
    for morsel in morsels(session, sql):
        values.extend(morsel.column(name).to_pylist())
    return values


class TestEndToEnd:
    """Real statements through opteryx-core, unpatched: no fixture stands
    between the query and whatever IcebergDataset.scan hands the engine. These
    all used to fail on defect 1 (the file:// path reaching rugo) before any
    result was produced; they now exercise real query semantics.
    """

    def test_select_star_returns_every_row(self, session):
        assert row_count(session, f"SELECT * FROM {TABLE}") == ROW_COUNT

    def test_select_star_returns_every_column(self, session):
        (morsel,) = morsels(session, f"SELECT * FROM {TABLE}")
        # Morsel.column_names is bytes for EVERY relation the engine reads -
        # `SELECT * FROM $planets` gives [b'id', b'name', ...] too - so the
        # decode is the engine's own surface, not an Iceberg-specific quirk.
        # The set and the order are still asserted exactly.
        assert [name.decode() for name in morsel.column_names] == list(ROWS)

    def test_aggregate_over_whole_table(self, session):
        sql = f"SELECT COUNT(*) AS n, SUM(i64) AS total, MAX(f64) AS biggest FROM {TABLE}"
        (morsel,) = morsels(session, sql)
        assert morsel.num_rows == 1
        assert morsel.column("n").to_pylist() == [7]
        assert morsel.column("total").to_pylist() == [28]  # 1+2+3+4+5+6+7
        assert morsel.column("biggest").to_pylist() == [9.5]

    def test_aggregate_with_group_by(self, session):
        sql = f"SELECT b, COUNT(*) AS n FROM {TABLE} GROUP BY b"
        counts = {}
        for morsel in morsels(session, sql):
            counts.update(dict(zip(morsel.column("b").to_pylist(), morsel.column("n").to_pylist())))
        assert counts == {True: 4, False: 3}

    def test_order_by_with_limit(self, session):
        sql = f"SELECT i64 FROM {TABLE} ORDER BY i64 DESC LIMIT 3"
        assert column(session, sql, "i64") == [7, 6, 5]

    def test_order_by_ascending_on_varchar_with_limit(self, session):
        sql = f"SELECT s FROM {TABLE} ORDER BY s ASC LIMIT 2"
        assert column(session, sql, "s") == ["ada", "alan"]

    @pytest.mark.parametrize("predicate, expected", PREDICATE_PARAMS)
    def test_predicate_row_count(self, session, predicate, expected):
        assert row_count(session, f"SELECT * FROM {TABLE} WHERE {predicate}") == expected


class TestPredicatePruning:
    """The same predicates with defect 1 held off, isolating defect 2.

    A failure here is a wrong ANSWER, not an error - the read path worked and
    the engine still returned the wrong number of rows. Anything that fails
    here is not attributable to the file:// scheme. Today only the DECIMAL
    equality case is wrong (defect 3 in the module docstring, carried as a
    strict xfail); everything else passes, and a NEW failure here means a real
    regression in this package's type mapping or bounds, not a known defect.
    """

    @pytest.mark.parametrize("predicate, expected", PREDICATE_PARAMS)
    def test_predicate_row_count(self, session, strip_file_scheme, predicate, expected):
        actual = row_count(session, f"SELECT * FROM {TABLE} WHERE {predicate}")
        assert actual == expected, (
            f"{predicate!r} returned {actual} rows, expected {expected} - the "
            f"datafile was read successfully, so rows were dropped by predicate "
            f"handling (manifest-bounds pruning), not by the file:// scheme"
        )

    def test_unfiltered_read_is_unaffected(self, session, strip_file_scheme):
        """Control: with defect 1 out of the way and no predicate to prune on,
        the full table reads back. This one is expected to PASS today - if it
        starts failing, something beyond the two known defects is broken and
        the attribution above no longer holds.
        """
        assert row_count(session, f"SELECT * FROM {TABLE}") == ROW_COUNT
