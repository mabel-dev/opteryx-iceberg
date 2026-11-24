"""A tenant's catalog cannot point Opteryx's identity at files that are not the tenant's.

Every Iceberg file - metadata, manifests and the data files opteryx-core scans - is
opened as Opteryx (opteryx_iceberg.fileio). A workspace owner writes their own
catalog, so its manifests can name ANY location: the worker's local disk, or a
bucket holding other workspaces' native data. These tests pin that a TENANT's
catalog (IcebergMetastore) is held to gs:// and s3:// objects outside the
deployment's reserved buckets - for reads, for the data files handed to the
engine, and for writes - while an operator's catalog keeps every location.
"""

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))

import pytest

from opteryx_iceberg import IcebergMetastore
from opteryx_iceberg import UnrestrictedIcebergMetastore
from opteryx_iceberg import fileio
from opteryx_iceberg.dataset import IcebergDataset
from opteryx_iceberg.fileio import RESERVED_BUCKETS_ENV
from opteryx_iceberg.fileio import AnyLocation
from opteryx_iceberg.fileio import IcebergFileIO
from opteryx_iceberg.fileio import OpteryxFileIO
from opteryx_iceberg.fileio import TenantFileIO
from opteryx_iceberg.fileio import TenantLocations
from opteryx_iceberg.manifests import ManifestDataFile
from opteryx_iceberg.metastore import _GuardedRestCatalog

RESERVED = "gs://opteryx_data, s3://opteryx-results,gs://shared-name"


@pytest.fixture
def reserved(monkeypatch):
    monkeypatch.setenv(RESERVED_BUCKETS_ENV, RESERVED)


@pytest.fixture
def no_storage(monkeypatch):
    """Fail the test if anything reaches a filesystem: a refusal must come first."""
    touched = MagicMock(side_effect=AssertionError("a refused location reached storage"))
    monkeypatch.setattr(fileio, "_filesystem", touched)
    monkeypatch.setattr(fileio, "_catalog_io", touched)
    return touched


# ── the policy ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "location",
    [
        "/etc/passwd",
        "relative/data.parquet",
        "file:///etc/passwd",
        "file://localhost/etc/passwd",
        "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
        "https://storage.googleapis.com/opteryx_data/ws/t/data/0.parquet",
        "hdfs://namenode/x.parquet",
        "GS://opteryx_data/x.parquet",
    ],
)
def test_a_tenant_catalog_may_not_name_anything_but_object_storage(reserved, location):
    with pytest.raises(PermissionError, match="only name gs:// or s3://"):
        TenantLocations().check(location)


@pytest.mark.parametrize(
    "location",
    [
        "gs://opteryx_data/other_workspace/sales/orders/data/0.parquet",
        "gcs://opteryx_data/other_workspace/x.parquet",
        "s3://opteryx-results/job/0.parquet",
        "s3a://opteryx-results/job/0.parquet",
        "s3n://opteryx-results/job/0.parquet",
        "gs://opteryx_data",
    ],
)
def test_a_tenant_catalog_may_not_name_a_reserved_bucket(reserved, location):
    with pytest.raises(PermissionError, match="belongs to this deployment"):
        TenantLocations().check(location)


@pytest.mark.parametrize(
    "location",
    [
        # Each spells a reserved bucket in a form a store might resolve to it; the
        # name is refused, never normalised, so no spelling can slip past the list.
        "gs://Opteryx_Data/x.parquet",
        "gs://opteryx%5Fdata/x.parquet",
        "gs://user@opteryx_data/x.parquet",
        "gs:///opteryx_data/x.parquet",
        "gs://opteryx_data:443/x.parquet",
        "s3://opteryx-results./x.parquet",
    ],
)
def test_a_bucket_name_that_is_not_canonical_is_refused(reserved, location):
    with pytest.raises(ValueError, match="valid"):
        TenantLocations().check(location)


@pytest.mark.parametrize(
    "location",
    [
        "gs://customer-lake/warehouse/db/t/metadata/snap-1.avro",
        "gs://customer_lake/x.parquet",
        "s3://customer.lake/warehouse/x.parquet",
        # the reserved list is per protocol: a gs:// bucket of ours says nothing
        # about an s3:// bucket that happens to share its name
        "s3://shared-name/x.parquet",
    ],
)
def test_a_tenant_catalog_may_name_its_own_buckets(reserved, location):
    TenantLocations().check(location)


def test_an_unset_reserved_list_refuses_every_tenant_location(monkeypatch):
    """Unset would mean nothing is protected, so it is never read as empty."""
    monkeypatch.delenv(RESERVED_BUCKETS_ENV, raising=False)
    with pytest.raises(RuntimeError, match=f"{RESERVED_BUCKETS_ENV} is not set"):
        TenantLocations().check("gs://customer-lake/x.parquet")


@pytest.mark.parametrize(
    "value", ["", " ", "opteryx_data", "gs://", "https://opteryx_data", "gs://a/b"]
)
def test_a_malformed_reserved_list_is_refused(monkeypatch, value):
    monkeypatch.setenv(RESERVED_BUCKETS_ENV, value)
    with pytest.raises(RuntimeError, match="is not gs://<bucket> or s3://<bucket>"):
        TenantLocations().check("gs://customer-lake/x.parquet")


def test_an_operator_catalog_keeps_every_location():
    for location in ("/var/lake/x.parquet", "file:///var/lake/x.parquet", "gs://opteryx_data/x"):
        AnyLocation().check(location)


# ── reads and writes through the FileIO ───────────────────────────────────────


@pytest.mark.parametrize("location", ["/etc/passwd", "gs://opteryx_data/ws/t/metadata/snap.avro"])
def test_a_tenant_read_is_refused_before_storage_is_touched(reserved, no_storage, location):
    with pytest.raises(PermissionError):
        TenantFileIO({}).new_input(location).open()
    no_storage.assert_not_called()


@pytest.mark.parametrize(
    "location", ["file:///tmp/x.avro", "gs://opteryx_data/ws/t/data/0.parquet"]
)
def test_a_tenant_write_is_refused_before_storage_is_touched(reserved, no_storage, location):
    with pytest.raises(PermissionError):
        TenantFileIO({}).new_output(location).create()
    with pytest.raises(PermissionError):
        TenantFileIO({}).delete(location)
    with pytest.raises(PermissionError):
        IcebergFileIO({}, TenantLocations()).new_output(location).create()
    no_storage.assert_not_called()


def test_an_output_files_read_back_keeps_the_tenant_policy(reserved, no_storage):
    output = TenantFileIO({}).new_output("gs://opteryx_data/ws/t/metadata/v1.json")
    with pytest.raises(PermissionError):
        output.to_input_file().open()


def test_an_operator_read_of_a_local_file_still_works(tmp_path):
    path = tmp_path / "m.avro"
    path.write_bytes(b"manifest")
    assert OpteryxFileIO({}).new_input(str(path)).open().read() == b"manifest"


# ── the data files handed to opteryx-core ─────────────────────────────────────


def _dataset(io, file_path):
    """An IcebergDataset over one snapshot whose manifest names `file_path`."""
    table = MagicMock()
    table.schema.return_value = SimpleNamespace(fields=[])
    table.schemas.return_value = {}
    data_file = ManifestDataFile(file_path, "PARQUET", 10, 100, ([], [], [], []))
    planned = patch("opteryx_iceberg.dataset.plan_data_files", return_value=[data_file])
    return IcebergDataset("ns.t", table, io), planned


@pytest.mark.parametrize(
    "file_path",
    ["gs://opteryx_data/other_workspace/sales/orders/data/0.parquet", "file:///etc/passwd"],
)
def test_a_tenant_manifest_cannot_hand_the_engine_a_file_that_is_not_the_tenants(
    reserved, file_path
):
    """The data files are read by opteryx-core's scan, not through the FileIO, so
    this is the only place their location is decided."""
    dataset, planned = _dataset(TenantFileIO({}), file_path)
    with planned, pytest.raises(PermissionError):
        list(dataset._manifest_entries(None))


def test_a_tenant_manifest_naming_the_tenants_own_bucket_is_served(reserved):
    dataset, planned = _dataset(TenantFileIO({}), "gs://customer-lake/t/data/0.parquet")
    with planned:
        (entry,) = dataset._manifest_entries(None)
    assert entry["file_path"] == "gs://customer-lake/t/data/0.parquet"


def test_an_operator_manifest_may_name_local_data_files():
    dataset, planned = _dataset(OpteryxFileIO({}), "file:///var/lake/t/data/0.parquet")
    with planned:
        (entry,) = dataset._manifest_entries(None)
    assert entry["file_path"] == "/var/lake/t/data/0.parquet"


# ── which metastore gets which ────────────────────────────────────────────────

BIGLAKE = {
    "uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
    "warehouse": "gs://customer-lake",
    "auth": {"type": "google"},
}


def _metastore(cls, **properties):
    # each class's own REST catalog: _TenantRestCatalog / _GuardedRestCatalog
    target = "_TenantRestCatalog" if cls is IcebergMetastore else "_GuardedRestCatalog"
    with patch(f"opteryx_iceberg.metastore.{target}") as construct:
        metastore = cls(workspace="ws", **properties)
    metastore._catalog = MagicMock()
    metastore._catalog.load_table.return_value = SimpleNamespace(io=SimpleNamespace(properties={}))
    return construct, metastore


def test_a_tenant_metastore_reads_and_writes_through_tenant_locations():
    construct, metastore = _metastore(IcebergMetastore, **BIGLAKE)
    assert construct.call_args.kwargs["py-io-impl"] == "opteryx_iceberg.fileio.TenantFileIO"
    assert type(metastore.load_dataset("ns.t")._io) is TenantFileIO
    assert type(metastore.io._reader.policy) is TenantLocations
    assert type(metastore.io._writer.policy) is TenantLocations


def test_an_operator_metastore_keeps_every_location():
    construct, metastore = _metastore(
        UnrestrictedIcebergMetastore, uri="http://localhost:8181/catalog", warehouse="w"
    )
    assert construct.call_args.kwargs["py-io-impl"] == "opteryx_iceberg.fileio.OpteryxFileIO"
    assert type(metastore.load_dataset("ns.t")._io) is OpteryxFileIO
    assert type(metastore.io._reader.policy) is AnyLocation


@pytest.fixture
def catalog_server():
    # the same 127.0.0.1 REST catalog test_catalog_properties pins the /v1/config guard with
    from test_catalog_properties import _FakeRestCatalog

    server = _FakeRestCatalog({"overrides": {"prefix": "p"}})
    yield server.uri
    server.close()


def test_a_served_py_io_impl_cannot_swap_a_tenant_catalog_back_to_the_operator_fileio(
    catalog_server,
):
    """pyiceberg builds a table's FileIO from properties the catalog's owner serves;
    naming OpteryxFileIO there would lift the tenant's location policy."""
    catalog = _GuardedRestCatalog(
        "ws", uri=catalog_server, warehouse="w", **{"py-io-impl": fileio.TENANT_PY_IO_IMPL}
    )
    io = catalog._load_file_io({"py-io-impl": fileio.PY_IO_IMPL}, "gs://b/m.json")
    assert type(io) is TenantFileIO
