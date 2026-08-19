"""Regression test: IcebergMetastore's own kwargs must stay flat/hashable.

register_workspace stores its kwargs verbatim, and opteryx-core's connector
cache hashes them (tuple(sorted(connector_entry.items())) in
connectors/__init__.py's connector_factory) - a dict value there raises
TypeError: unhashable type: 'dict'. This caught a real instance of that: an
earlier version of IcebergMetastore accepted pyiceberg's own nested
auth={"type": "google", "google": {...}} dict directly, which broke
registration through opteryx-core end to end (see opteryx-iceberg's README
for the full repro). Verify the nested dict is only ever built internally,
never accepted as an incoming kwarg.
"""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))

from opteryx_iceberg import IcebergMetastore


def test_all_init_kwargs_are_hashable():
    kwargs = {
        "workspace": "tarchia",
        "catalog_type": "rest",
        "uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
        "warehouse": "bl://projects/example/catalogs/example",
        "auth_type": "google",
        "google_auth_scopes": ("https://www.googleapis.com/auth/cloud-platform",),
        "header.x-goog-user-project": "example",
    }
    # This is exactly what opteryx-core's connector_factory does with
    # whatever register_workspace was called with - must not raise.
    tuple(sorted(kwargs.items()))


def test_google_auth_type_builds_nested_pyiceberg_config():
    captured = {}

    def fake_load_catalog(name, **properties):
        captured.update(properties)
        return object()

    with patch("opteryx_iceberg.metastore.load_catalog", fake_load_catalog):
        IcebergMetastore(
            workspace="tarchia",
            catalog_type="rest",
            uri="https://biglake.googleapis.com/iceberg/v1/restcatalog",
            auth_type="google",
            google_auth_scopes=("https://www.googleapis.com/auth/cloud-platform",),
        )

    assert captured["auth"] == {
        "type": "google",
        "google": {"scopes": ["https://www.googleapis.com/auth/cloud-platform"]},
    }
    # auth_type/google_auth_scopes are consumed here, not forwarded raw.
    assert "auth_type" not in captured
    assert "google_auth_scopes" not in captured


def test_unknown_auth_type_raises():
    try:
        IcebergMetastore(workspace="x", auth_type="not-a-real-type")
        raise AssertionError("expected NotImplementedError")
    except NotImplementedError:
        pass
