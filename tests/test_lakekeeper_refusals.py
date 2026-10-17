"""opteryx-iceberg's identity check, against a real catalog that trips it.

Metadata and data are read through opteryx-core, with opteryx-core's own S3
endpoint and key (opteryx_iceberg.fileio). A catalog that serves a DIFFERENT
endpoint, or vends a different key, expects the read to happen somewhere else
or as someone else - so the read is refused, not quietly served under
opteryx-core's identity. Two of the stack's warehouses (scripts/lakekeeper.sh)
do exactly that:

  LAKEKEEPER_ENDPOINT_MISMATCH_WAREHOUSE  profile endpoint http://127.0.0.1:9000,
                                          the same Silo as AWS_S3_ENDPOINT
                                          (http://localhost:9000) under another
                                          string
  LAKEKEEPER_STS_WAREHOUSE                STS on: every table load vends a
                                          temporary key

Each table is written by plain pyiceberg (which takes whatever the warehouse
serves), so it is readable - just not by an identity opteryx-iceberg will use.
Skips without the stack, fails without it under LAKEKEEPER_REQUIRED.
"""

import os
import uuid

import pyarrow as pa
import pytest
from _lakekeeper import require_stack
from _lakekeeper import rows

require_stack()

import opteryx
from opteryx.connectors import register_workspace
from opteryx.connectors.opteryx_connector import OpteryxConnector
from pyiceberg.catalog import load_catalog

from opteryx_iceberg import IcebergMetastore

NAMESPACE = f"ci_{uuid.uuid4().hex[:12]}"

# workspace -> (warehouse env var, the message the refusal must carry)
REFUSING = {
    "lakekeeper_endpoint": (
        "LAKEKEEPER_ENDPOINT_MISMATCH_WAREHOUSE",
        "Iceberg properties name S3 endpoint 'http://127.0.0.1:9000'",
    ),
    "lakekeeper_sts": (
        "LAKEKEEPER_STS_WAREHOUSE",
        "Iceberg properties carry an S3 access key that differs from opteryx-core's",
    ),
}


def _properties(workspace):
    return {
        "uri": f"{os.environ['LAKEKEEPER_URI']}/catalog",
        "warehouse": os.environ[REFUSING[workspace][0]],
    }


@pytest.fixture(scope="module", params=sorted(REFUSING))
def refusing(request):
    workspace = request.param
    # No s3.* properties: the writer takes the endpoint and (for STS) the
    # vended key the warehouse serves - the identity under test.
    writer = load_catalog(f"{workspace}_writer", type="rest", **_properties(workspace))
    writer.create_namespace(NAMESPACE)
    table = writer.create_table(f"{NAMESPACE}.t", schema=pa.schema([pa.field("id", pa.int64())]))
    table.append(pa.table({"id": pa.array([1, 2, 3], pa.int64())}))

    register_workspace(
        workspace,
        OpteryxConnector,
        catalog=IcebergMetastore,
        catalog_type="rest",
        **_properties(workspace),
    )
    yield {
        "workspace": workspace,
        "table": table,
        "name": f"{workspace}.{NAMESPACE}.t",
        "message": REFUSING[workspace][1],
    }

    writer.drop_table(f"{NAMESPACE}.t")
    writer.drop_namespace(NAMESPACE)


def test_the_warehouse_really_serves_another_identity(refusing):
    """Precondition: without it, the refusal below would be testing nothing."""
    served = refusing["table"].io.properties
    if refusing["workspace"] == "lakekeeper_endpoint":
        assert served["s3.endpoint"].rstrip("/") == "http://127.0.0.1:9000"
        assert served["s3.endpoint"].rstrip("/") != os.environ["AWS_S3_ENDPOINT"].rstrip("/")
    else:
        assert served["s3.access-key-id"] != os.environ["AWS_ACCESS_KEY_ID"]


def test_the_read_is_refused_with_the_cause(refusing):
    with pytest.raises(Exception) as caught:
        rows(opteryx.session(user="tests"), f"SELECT COUNT(*) FROM {refusing['name']}")
    assert refusing["message"] in str(caught.value)
