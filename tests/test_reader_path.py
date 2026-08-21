"""Unit coverage for the datafile-location rewrite in IcebergDataset.scan.

tests/test_end_to_end_sql.py proves the local case end to end - a "file://"
warehouse now reads through the engine - but it can only ever exercise the one
scheme a local SqlCatalog writes. These pin the cases a local fixture cannot
reach: object-store locations must survive untouched (rewriting "gs://" to a
plain path would send every cloud warehouse to the local filesystem), and a
percent-encoded path must be decoded, since that is what a URI's path means.
"""

from opteryx_iceberg.dataset import _reader_path


def test_local_uri_loses_its_scheme():
    assert _reader_path("file:///warehouse/ns/t/data/00000-0-abc.parquet") == (
        "/warehouse/ns/t/data/00000-0-abc.parquet"
    )


def test_localhost_authority_is_still_local():
    assert _reader_path("file://localhost/warehouse/t/x.parquet") == "/warehouse/t/x.parquet"


def test_percent_encoding_is_decoded():
    # pyiceberg writes a warehouse path containing a space as %20; the reader
    # opens a real path, so it has to arrive as a space.
    assert _reader_path("file:///ware%20house/ns/t/x.parquet") == "/ware house/ns/t/x.parquet"


def test_object_store_locations_pass_through():
    for location in (
        "gs://bucket/ns/t/data/x.parquet",
        "s3://bucket/ns/t/data/x.parquet",
        "abfss://container@acct.dfs.core.windows.net/t/x.parquet",
    ):
        assert _reader_path(location) == location


def test_remote_share_is_not_reinterpreted_as_local():
    # file://host/share is another machine's file - silently turning it into
    # /share would read an unrelated local path, so it is left as-is.
    assert _reader_path("file://fileserver/share/t/x.parquet") == (
        "file://fileserver/share/t/x.parquet"
    )


def test_plain_path_is_unchanged():
    assert _reader_path("/warehouse/ns/t/x.parquet") == "/warehouse/ns/t/x.parquet"
