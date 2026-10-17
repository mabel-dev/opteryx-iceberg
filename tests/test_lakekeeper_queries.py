"""A query battery over real data in Lakekeeper, checked against local disk.

opteryx-core's own test data is loaded into Lakekeeper as Iceberg tables, and
every query here runs twice through the same engine: once over the Iceberg
tables (REST catalog, s3:// metadata and data, opteryx-iceberg) and once over
the original parquet on local disk. The local outcome is the oracle; the only
variable is the route, so any difference is a defect on the Iceberg path.

Suites, each in its own namespace (TPC-H and TPC-DS both have `customer`):

  misc        planets, satellites, missions - handwritten queries below
  tpch        TPC-H sf0.01 - core's 22 queries (tests/performance/tpch)
  tpcds       TPC-DS sf0.01 - core's 99 queries (tests/performance/tpcds)
  clickbench  clickbench_tiny (1M rows of hits) - core's 43 statements
              (tests/performance/clickbench/opteryx/runner.py)

Data and SQL are read from the opteryx-core checkout named by
OPTERYX_CORE_ROOT - they have one home, there. Every query runs against both
of the stack's warehouses (see tests/_lakekeeper.py). astronauts is left out:
it has list columns, which opteryx-iceberg refuses (see the README).

The OUTCOME must match: the same rows, row for row and in order, or the same
error type (a query core cannot run locally must fail the same way over
Iceberg, not succeed or fail differently). An empty answer still has to agree.
A query whose ORDER BY leaves ties could compare unequal without being wrong
(unordered output is normal SQL); if one fails on row order alone, that is the
query's fault - do not loosen the comparison.
"""

import ast
import contextlib
import glob
import math
import os
import re
import uuid

import pytest
from _lakekeeper import WAREHOUSES
from _lakekeeper import arrow
from _lakekeeper import require_stack
from _lakekeeper import rest_properties
from _lakekeeper import rows
from _lakekeeper import writable
from _lakekeeper import writer as make_writer

require_stack()

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector

from opteryx_iceberg import IcebergMetastore

CORE_ROOT = os.environ.get("OPTERYX_CORE_ROOT")
if not CORE_ROOT:
    raise RuntimeError(
        "OPTERYX_CORE_ROOT must name an opteryx-core checkout: its testdata/ and TPC-H "
        "queries are this battery's data and SQL."
    )

RUN = f"ci_{uuid.uuid4().hex[:12]}"

# (id, template) - `{table}` placeholders name the suite's tables.
HANDWRITTEN = [
    ("planets_all", "SELECT * FROM {planets} ORDER BY id"),
    ("planets_filter", "SELECT name, mass FROM {planets} WHERE mass > 5 ORDER BY name"),
    (
        "planets_aggregates",
        "SELECT COUNT(*), SUM(number_of_moons), MIN(gravity), MAX(gravity), "
        "MIN(surface_pressure), MAX(orbital_eccentricity) FROM {planets}",
    ),
    (
        "planets_decimal_predicate",
        "SELECT id, gravity FROM {planets} WHERE gravity BETWEEN 3 AND 10 ORDER BY id",
    ),
    ("planets_like", "SELECT name FROM {planets} WHERE name LIKE '%a%' ORDER BY name"),
    (
        "planets_nulls",
        "SELECT name FROM {planets} WHERE surface_pressure IS NULL ORDER BY name",
    ),
    (
        "satellites_group_by",
        "SELECT planetId, COUNT(*) AS n, MAX(radius) AS r FROM {satellites} "
        "GROUP BY planetId ORDER BY planetId",
    ),
    (
        "satellites_top_n",
        "SELECT id, name, radius FROM {satellites} ORDER BY radius DESC, id LIMIT 10",
    ),
    (
        "satellites_nulls",
        "SELECT COUNT(*) FROM {satellites} WHERE magnitude IS NULL",
    ),
    (
        "planets_left_join_satellites",
        "SELECT p.name, COUNT(s.id) AS moons FROM {planets} AS p "
        "LEFT JOIN {satellites} AS s ON p.id = s.planetId GROUP BY p.name ORDER BY p.name",
    ),
    (
        "planets_inner_join_satellites",
        "SELECT p.name AS planet, s.name AS moon FROM {planets} AS p "
        "INNER JOIN {satellites} AS s ON p.id = s.planetId WHERE s.radius > 100 "
        "ORDER BY s.name",
    ),
    (
        "missions_by_company",
        "SELECT Company, COUNT(*) AS n FROM {missions} GROUP BY Company ORDER BY Company",
    ),
    (
        "missions_price_by_status",
        "SELECT Mission_Status, COUNT(Price) AS priced, MIN(Price), MAX(Price) "
        "FROM {missions} GROUP BY Mission_Status ORDER BY Mission_Status",
    ),
    (
        "missions_timestamp_predicate",
        "SELECT Company, Mission, Lauched_at FROM {missions} "
        "WHERE Lauched_at >= '2020-01-01'::TIMESTAMP ORDER BY Lauched_at, Company, Mission",
    ),
    (
        "missions_by_year",
        "SELECT EXTRACT(year FROM Lauched_at) AS y, COUNT(*) AS n FROM {missions} "
        "GROUP BY EXTRACT(year FROM Lauched_at) ORDER BY y",
    ),
    (
        "missions_distinct",
        "SELECT DISTINCT Rocket_Status FROM {missions} ORDER BY Rocket_Status",
    ),
    ("missions_null_price", "SELECT COUNT(*) FROM {missions} WHERE Price IS NULL"),
]


def _sql_files(relative, expected_count, local_prefix, suite):
    """Core's query files, with `<local_prefix>.<table>` made `{<table>}`."""
    pattern = os.path.join(CORE_ROOT, relative, "*.sql")
    paths = sorted(glob.glob(pattern))
    if len(paths) != expected_count:
        raise RuntimeError(f"expected {expected_count} queries at {pattern}, found {len(paths)}")
    queries = []
    for path in paths:
        with open(path) as handle:
            sql = handle.read()
        template = re.sub(re.escape(local_prefix) + r"\.([a-z_]+)", r"{\1}", sql)
        queries.append((f"{suite}_{os.path.basename(path)[:-4]}", template))
    return queries


def _clickbench_statements():
    """ClickBench's STATEMENTS, read from core's runner WITHOUT importing it -
    the module pops OPTERYX_DEBUG and edits sys.path at import."""
    path = os.path.join(CORE_ROOT, "tests", "performance", "clickbench", "opteryx", "runner.py")
    with open(path) as handle:
        module = ast.parse(handle.read())
    for node in module.body:
        if isinstance(node, ast.Assign) and [t.id for t in node.targets] == ["STATEMENTS"]:
            statements = ast.literal_eval(node.value)
            break
    else:
        raise RuntimeError(f"no STATEMENTS assignment in {path}")
    if len(statements) != 43:
        raise RuntimeError(f"expected the 43 ClickBench statements in {path}, found {len(statements)}")
    return [
        (f"clickbench_q{index:02d}", sql.replace("{DATASET}", "{hits}"))
        for index, (sql, _) in enumerate(statements)
    ]


def _tpcds_tables():
    names = sorted(os.listdir(os.path.join(CORE_ROOT, "testdata", "tpcds_001")))
    if len(names) != 24:
        raise RuntimeError(f"expected the 24 TPC-DS tables in testdata/tpcds_001, found {len(names)}")
    return names


TPCH_TABLES = ("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

# suite -> (Iceberg table name -> the same data on local disk, as a dotted name
# resolved against CORE_ROOT; core's filesystem connector maps a.b.c to a/b/c),
# and the suite's (id, template) queries.
SUITES = {
    "misc": (
        {
            "planets": "testdata.planets",
            "satellites": "testdata.satellites",
            "missions": "testdata.missions",
        },
        HANDWRITTEN,
    ),
    "tpch": (
        {table: f"testdata.tpch_001.{table}" for table in TPCH_TABLES},
        _sql_files("tests/performance/tpch/opteryx/queries", 22, "testdata.tpch", "tpch"),
    ),
    "tpcds": (
        {table: f"testdata.tpcds_001.{table}" for table in _tpcds_tables()},
        _sql_files("tests/performance/tpcds/opteryx/queries", 99, "testdata.tpcds_tiny", "tpcds"),
    ),
    "clickbench": ({"hits": "testdata.clickbench_tiny"}, _clickbench_statements()),
}

CASES = [
    pytest.param(suite, query_id, template, id=query_id)
    for suite, (_, queries) in SUITES.items()
    for query_id, template in queries
]


# ORDER BY ... LIMIT queries whose sort key ties across the LIMIT boundary:
# which tied rows are kept is arbitrary, and core's local answer is itself not
# the same from run to run. This list is exactly the queries whose local answer
# varied across five local runs of every query in every suite (2026-10-17) -
# measured, not guessed; add to it only the same way. For these the check is
# tie-aware: the sort-key sequence must match exactly, and every row returned
# must be a row of the full (LIMIT-free) local answer. id -> the sort key's
# output column.
TIED_AT_LIMIT = {
    "clickbench_q10": "u",
    "clickbench_q11": "u",
    "clickbench_q15": "COUNT(*)",
    "clickbench_q16": "COUNT(*)",
    "clickbench_q18": "COUNT(*)",
    "clickbench_q30": "c",
    "clickbench_q31": "c",
    "clickbench_q32": "c",
}

# DOUBLE results are compared to this relative tolerance; everything else
# (DECIMAL, integers, text, dates) exactly. Float summation order varies with
# parallelism: core's own local answer to tpcds_query75 differs in the last
# digits from run to run (-133143.13999999996 / -133143.14 / -133143.1399999999).
FLOAT_REL_TOL = 1e-9


def _same_value(a, b):
    if isinstance(a, float) and isinstance(b, float):
        return (math.isnan(a) and math.isnan(b)) or math.isclose(a, b, rel_tol=FLOAT_REL_TOL)
    return a == b


def _same_rows(actual, expected):
    return len(actual) == len(expected) and all(
        a.keys() == e.keys() and all(_same_value(a[k], e[k]) for k in a)
        for a, e in zip(actual, expected)
    )


def _local_names(result, iceberg_refs, local_refs):
    """An unaliased qualified column is named after its relation, so the same
    column is `testdata.tpcds_001.item.i_category_id` locally and
    `<workspace>.<namespace>.item.i_category_id` over Iceberg. Map the Iceberg
    references in output names back to the local ones (longest first)."""
    pairs = sorted(
        ((iceberg_refs[t], local_refs[t]) for t in iceberg_refs), key=lambda p: -len(p[0])
    )

    def rename(name):
        for iceberg_ref, local_ref in pairs:
            name = name.replace(iceberg_ref, local_ref)
        return name

    return [{rename(k): v for k, v in row.items()} for row in result]


def _render(template, refs):
    """`{table}` -> refs[table]. Only the suite's table names are replaced, so
    any other brace in the SQL is left alone."""
    return re.sub(r"\{(\w+)\}", lambda m: refs.get(m.group(1), m.group(0)), template)


def _local(sql):
    with contextlib.chdir(CORE_ROOT):
        return arrow(opteryx.session(), sql)


@pytest.fixture(scope="module", params=sorted(WAREHOUSES))
def warehouse(request):
    """Every suite's tables, written into one Lakekeeper warehouse (one
    namespace per suite) and registered as an Opteryx workspace."""
    workspace = request.param
    writer = make_writer(workspace)
    refs = {}
    for suite, (tables, _) in SUITES.items():
        namespace = f"{RUN}_{suite}"
        writer.create_namespace(namespace)
        for table, local in tables.items():
            data = _local(f"SELECT * FROM {local}")
            writable(writer.create_table(f"{namespace}.{table}", schema=data.schema)).append(data)
        refs[suite] = {table: f"{workspace}.{namespace}.{table}" for table in tables}

    register_workspace(
        workspace,
        OpteryxConnector,
        catalog=IcebergMetastore,
        catalog_type="rest",
        **rest_properties(workspace),
    )
    yield {"session": opteryx.session(user="tests"), "refs": refs}

    for suite, (tables, _) in SUITES.items():
        namespace = f"{RUN}_{suite}"
        for table in tables:
            writer.drop_table(f"{namespace}.{table}")
        writer.drop_namespace(namespace)


def test_every_table_round_trips(warehouse):
    """The load itself: same row count on both sides, before any query is
    trusted to mean something."""
    for suite, (tables, _) in SUITES.items():
        for table, local in tables.items():
            expected = _local(f"SELECT COUNT(*) AS n FROM {local}").to_pylist()
            actual = rows(
                warehouse["session"], f"SELECT COUNT(*) AS n FROM {warehouse['refs'][suite][table]}"
            )
            assert actual == expected, f"{suite}.{table}"


@pytest.mark.parametrize("suite, query_id, template", CASES)
def test_matches_local_disk(warehouse, suite, query_id, template):
    local_refs = SUITES[suite][0]
    iceberg_refs = warehouse["refs"][suite]
    local_sql = _render(template, local_refs)
    iceberg_sql = _render(template, iceberg_refs)
    local_error = None
    try:
        expected = _local(local_sql)
    except Exception as error:  # the oracle's own failure is the expected outcome
        local_error = error
    if local_error is not None:
        with pytest.raises(type(local_error)):
            rows(warehouse["session"], iceberg_sql)
        return
    expected = [] if expected is None else expected.to_pylist()
    actual = _local_names(rows(warehouse["session"], iceberg_sql), iceberg_refs, local_refs)

    key = TIED_AT_LIMIT.get(query_id)
    if key is None:
        assert _same_rows(actual, expected)
        return
    assert [row[key] for row in actual] == [row[key] for row in expected]
    unlimited = _local(re.sub(r"\bLIMIT\s+\d+", "", local_sql, flags=re.IGNORECASE)).to_pylist()
    full = {repr(sorted(row.items())) for row in unlimited}
    assert all(repr(sorted(row.items())) in full for row in actual)
