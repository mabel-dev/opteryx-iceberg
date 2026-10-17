"""Shared plumbing for the tests that run against scripts/lakekeeper.sh's stack.

`require_stack()` is the one skip/fail rule: a test module calls it before
anything else, and it SKIPS that module when LAKEKEEPER_URI is unset (a dev
machine without Docker) - unless LAKEKEEPER_REQUIRED is set, as in CI, where a
missing stack is a failure. CI must never pass these modules by skipping them.
"""

import os

import pyarrow as pa
import pytest

# workspace prefix -> the env var naming its Lakekeeper warehouse (see
# scripts/lakekeeper.sh for what each one serves on a table load)
WAREHOUSES = {
    "lakekeeper": "LAKEKEEPER_WAREHOUSE",
    "lakekeeper_default": "LAKEKEEPER_DEFAULT_WAREHOUSE",
}


def require_stack():
    if os.environ.get("LAKEKEEPER_URI"):
        return
    if os.environ.get("LAKEKEEPER_REQUIRED"):
        raise RuntimeError(
            "LAKEKEEPER_REQUIRED is set but LAKEKEEPER_URI is not - load scripts/lakekeeper.env."
        )
    pytest.skip(
        "no Lakekeeper stack (LAKEKEEPER_URI unset) - see scripts/lakekeeper.sh",
        allow_module_level=True,
    )


def rest_properties(workspace):
    return {
        "uri": f"{os.environ['LAKEKEEPER_URI']}/catalog",
        "warehouse": os.environ[WAREHOUSES[workspace]],
    }


def _static_s3():
    return {
        "s3.endpoint": os.environ["AWS_S3_ENDPOINT"],
        "s3.access-key-id": os.environ["AWS_ACCESS_KEY_ID"],
        "s3.secret-access-key": os.environ["AWS_SECRET_ACCESS_KEY"],
        "s3.region": os.environ["AWS_REGION"],
        "s3.path-style-access": "true",
    }


def writer(workspace):
    """A plain pyiceberg RestCatalog that creates and commits test tables,
    independently of opteryx-iceberg. Write data through `writable(...)`."""
    from pyiceberg.catalog import load_catalog

    return load_catalog(f"{workspace}_writer", type="rest", **rest_properties(workspace))


def writable(table):
    """`table` with its files written by pyiceberg's PyArrowFileIO under the
    stack's static key; commits still go through Lakekeeper.

    The FileIO a table load SERVES is not used for writing: on
    `lakekeeper_default` it is FsspecFileIO behind Lakekeeper's remote signer,
    and Silo intermittently refused a signed PutObject from it (AccessDenied,
    with nothing in Lakekeeper's log) - a flaky fixture, in a path no test is
    about. What a load serves is still what opteryx-iceberg READS against, so
    keep the original table for asserting that. A commit leaves a pyiceberg
    Table's `io` alone, so this holds across appends, schema updates, tags and
    deletes."""
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyiceberg.table import Table

    return Table(
        identifier=table.name(),
        metadata=table.metadata,
        metadata_location=table.metadata_location,
        io=PyArrowFileIO(_static_s3()),
        catalog=table.catalog,
        config=table.config,
    )


def arrow(session, sql):
    tables = [m.to_arrow() for m in session.execute_to_morsels(sql) if m.num_rows]
    if not tables:
        return None
    return pa.concat_tables(tables) if len(tables) > 1 else tables[0]


def rows(session, sql):
    table = arrow(session, sql)
    return [] if table is None else table.to_pylist()


def scalar(session, sql):
    result = rows(session, sql)
    assert len(result) == 1
    (value,) = result[0].values()
    return value
