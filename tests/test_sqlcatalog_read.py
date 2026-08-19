"""Local sanity check (no server, no Docker): write an Iceberg table with
plain pyiceberg against its own SqlCatalog (SQLite metadata + local-disk
FileIO), then read it back through opteryx_iceberg.IcebergMetastore exactly
as opteryx-core's OpteryxConnector would.
"""

import os
import sys

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))

import pyarrow as pa
from opteryx_catalog.catalog.dataset import Datafile
from opteryx_catalog.exceptions import DatasetNotFound
from pyiceberg.catalog.sql import SqlCatalog

from opteryx_iceberg import IcebergMetastore


def _make_catalog(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    writer_catalog = SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
            "warehouse": f"file://{warehouse}",
        },
    )
    writer_catalog.create_namespace("ns")
    arrow_table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    table = writer_catalog.create_table("ns.people", schema=arrow_table.schema)
    table.append(arrow_table)
    return tmp_path, warehouse


def _make_metastore(tmp_path):
    return IcebergMetastore(
        workspace="test",
        catalog_type="sql",
        uri=f"sqlite:///{tmp_path / 'catalog.db'}",
        warehouse=f"file://{tmp_path / 'warehouse'}",
    )


def test_load_dataset_and_scan(tmp_path):
    _make_catalog(tmp_path)
    metastore = _make_metastore(tmp_path)

    dataset = metastore.load_dataset("ns.people")

    files = list(dataset.scan())
    assert len(files) == 1
    entry = files[0]
    assert isinstance(entry, Datafile)
    assert entry.record_count == 3
    assert entry.file_path.endswith(".parquet")


def test_schema_maps_types(tmp_path):
    _make_catalog(tmp_path)
    metastore = _make_metastore(tmp_path)

    schema = metastore.load_dataset("ns.people").schema()

    names = [c.name for c in schema.columns]
    assert names == ["id", "name"]
    assert schema.columns[1].type == "VARCHAR"


def test_load_dataset_missing_raises_catalog_not_found(tmp_path):
    _make_catalog(tmp_path)
    metastore = _make_metastore(tmp_path)

    try:
        metastore.load_dataset("ns.does_not_exist")
        raise AssertionError("expected DatasetNotFound")
    except DatasetNotFound:
        pass


def test_get_relation(tmp_path):
    _make_catalog(tmp_path)
    metastore = _make_metastore(tmp_path)

    kind, obj = metastore.get_relation("ns.people")
    assert kind == "dataset"
    assert obj.identifier == "ns.people"

    kind, obj = metastore.get_relation("ns.missing")
    assert (kind, obj) == (None, None)


def test_dataset_exists(tmp_path):
    _make_catalog(tmp_path)
    metastore = _make_metastore(tmp_path)

    assert metastore.dataset_exists("ns.people") is True
    assert metastore.dataset_exists("ns.missing") is False


def test_writes_are_not_implemented(tmp_path):
    _make_catalog(tmp_path)
    metastore = _make_metastore(tmp_path)

    try:
        metastore.create_dataset("ns.new", schema=None)
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass
