"""IcebergMetastore forwards config to pyiceberg verbatim - nesting included.

The flat `auth_type`/`google_auth_scopes` kwargs this file used to defend are
retired: they existed only because opteryx-core's connector cache hashed
registration kwargs, and a dict value broke the hash. That cache is now keyed
by workspace name (opteryx-core's resolution-first connector layer), so
pyiceberg's own nested config shapes pass straight through - and the old flat
spelling raises a clear error instead of silently producing unauthenticated
requests.
"""

import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx-catalog"))

import pytest

from opteryx_iceberg import IcebergMetastore

GOOGLE_AUTH = {
    "type": "google",
    "google": {"scopes": ["https://www.googleapis.com/auth/cloud-platform"]},
}


def _load_with(**kwargs):
    captured = {}

    def fake_load_catalog(name, **properties):
        captured["name"] = name
        captured["properties"] = properties
        return object()

    with patch("opteryx_iceberg.metastore.load_catalog", fake_load_catalog):
        IcebergMetastore(**kwargs)
    return captured


def test_nested_auth_passes_through_verbatim():
    captured = _load_with(
        workspace="tarchia",
        catalog_type="rest",
        uri="https://biglake.googleapis.com/iceberg/v1/restcatalog",
        auth=GOOGLE_AUTH,
        **{"header.x-goog-user-project": "example"},
    )
    assert captured["name"] == "tarchia"
    assert captured["properties"]["auth"] == GOOGLE_AUTH
    assert captured["properties"]["auth"] is not None  # not rebuilt, not dropped
    assert captured["properties"]["header.x-goog-user-project"] == "example"
    assert captured["properties"]["type"] == "rest"


def test_stored_credential_keys_pass_through():
    # pyiceberg understands `token`/`credential` natively - stored-credential
    # catalogs are configuration, not new code here.
    captured = _load_with(workspace="x", uri="https://example", token="tk", credential="id:secret")
    assert captured["properties"]["token"] == "tk"
    assert captured["properties"]["credential"] == "id:secret"


def test_retired_flat_auth_kwargs_raise():
    with pytest.raises(ValueError, match="auth_type.*retired"):
        IcebergMetastore(workspace="x", auth_type="google")
    with pytest.raises(ValueError, match="google_auth_scopes.*retired"):
        IcebergMetastore(workspace="x", google_auth_scopes=("s",))
