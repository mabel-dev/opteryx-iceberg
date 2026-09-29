"""Iceberg metadata reads through opteryx-core's own storage clients.

Two FileIO surfaces share ONE read path:

- `OpteryxFileIO` is a pyiceberg FileIO. IcebergMetastore installs it as the
  catalog's `py-io-impl`, so every table pyiceberg loads reads its manifest
  list and manifests - `table.scan().plan_files()` - through it.
- `IcebergFileIO` is the opteryx_catalog FileIO surface (`metastore.io`).

Each read is the whole object in ONE GET, via
`opteryx.connectors.io_systems.create_filesystem` - the same S3 (SigV4
presigned), GCS and local clients the engine's scans use. This replaces
pyiceberg's default PyArrowFileIO, which sends a HEAD (for the object size)
before every GET: two serial round trips per metadata file, where one does,
since Iceberg metadata files are always read whole. The clients are held per
scheme for the life of the process, so every table's reads share one
connection cache instead of dialling per table.

Credentials and endpoint come from opteryx-core's configuration (the process
environment / application default credentials), not from pyiceberg's
properties. Where the properties name a different S3 endpoint or access key,
or carry a vended GCS token, the read is refused rather than silently served
under a different identity.

Read-only: nothing is written through either surface.
"""

from __future__ import annotations

import threading
from io import BytesIO
from typing import BinaryIO

from opteryx.connectors.io_systems import create_filesystem
from opteryx_catalog.iops.base import FileIO
from opteryx_catalog.iops.base import InputFile as BaseInputFile
from pyiceberg.io import FileIO as PyIcebergFileIO
from pyiceberg.io import InputFile as PyIcebergInputFile
from pyiceberg.io import InputStream
from pyiceberg.io import OutputFile as PyIcebergOutputFile

from opteryx_iceberg.dataset import _reader_path

# pyiceberg's `py-io-impl` value for OpteryxFileIO - see IcebergMetastore.
PY_IO_IMPL = "opteryx_iceberg.fileio.OpteryxFileIO"

# Location scheme -> the opteryx-core filesystem protocol that reads it.
_PROTOCOL_BY_SCHEME = {
    "s3": "s3",
    "s3a": "s3",
    "s3n": "s3",
    "gs": "gs",
    "gcs": "gs",
    "file": "file",
    "": "file",
}

_filesystems: dict = {}
_filesystems_lock = threading.Lock()


def _filesystem(protocol: str):
    """The process's one opteryx-core filesystem for `protocol`."""
    with _filesystems_lock:
        filesystem = _filesystems.get(protocol)
        if filesystem is None:
            filesystem = create_filesystem(protocol)
            _filesystems[protocol] = filesystem
        return filesystem


def _resolve(location: str) -> tuple[str, str]:
    """(protocol, path in the form that protocol's filesystem opens)."""
    scheme, separator, remainder = location.partition("://")
    if not separator:
        scheme, remainder = "", location
    protocol = _PROTOCOL_BY_SCHEME.get(scheme)
    if protocol is None:
        raise ValueError(
            f"opteryx-iceberg cannot read {location!r}: scheme {scheme!r} is not supported "
            f"(supported: {sorted(s for s in _PROTOCOL_BY_SCHEME if s)})."
        )
    if protocol == "file":
        path = _reader_path(location)
        if path.startswith("file://"):
            raise ValueError(
                f"opteryx-iceberg cannot read {location!r}: a file:// location on another "
                "host is not a local file."
            )
        return protocol, path
    return protocol, f"{protocol}://{remainder}"


def _check_identity(protocol: str, filesystem, properties: dict) -> None:
    """Refuse a read the properties expect under a different identity/endpoint."""
    if protocol == "s3":
        endpoint = str(properties.get("s3.endpoint") or "").rstrip("/")
        if endpoint and endpoint != filesystem.endpoint:
            raise ValueError(
                f"Iceberg properties name S3 endpoint {endpoint!r} but opteryx-core's S3 "
                f"filesystem is configured for {filesystem.endpoint or 'AWS'!r} "
                "(AWS_S3_ENDPOINT). Metadata is read through opteryx-core, so the two must agree."
            )
        access_key = properties.get("s3.access-key-id")
        if access_key and access_key != filesystem.credentials.frozen()[0]:
            raise ValueError(
                "Iceberg properties carry an S3 access key that differs from opteryx-core's "
                "credentials (AWS_ACCESS_KEY_ID / credential chain). Metadata is read through "
                "opteryx-core, so the catalog's vended or configured key would be ignored."
            )
    elif protocol == "gs" and properties.get("gcs.oauth2.token"):
        raise ValueError(
            "Iceberg properties carry a vended GCS token (gcs.oauth2.token). Metadata is read "
            "through opteryx-core with application default credentials, so the vended token "
            "would be ignored."
        )


class _ObjectReader:
    """Whole-object reads for one set of Iceberg properties."""

    def __init__(self, properties: dict):
        self._properties = properties
        self._checked: set = set()

    def read(self, location: str) -> bytes:
        protocol, path = _resolve(location)
        filesystem = _filesystem(protocol)
        if protocol not in self._checked:
            _check_identity(protocol, filesystem, self._properties)
            self._checked.add(protocol)
        handle = filesystem.open_input_stream(path)
        try:
            return bytes(handle.memoryview)
        finally:
            handle.close()


# ── pyiceberg surface ────────────────────────────────────────────────────────


class OpteryxInputFile(PyIcebergInputFile):
    """An Iceberg metadata file, fetched whole on first use."""

    def __init__(self, location: str, reader: _ObjectReader):
        super().__init__(location)
        self._reader = reader
        self._content: bytes | None = None

    def _bytes(self) -> bytes:
        if self._content is None:
            self._content = self._reader.read(self.location)
        return self._content

    def __len__(self) -> int:
        return len(self._bytes())

    def exists(self) -> bool:
        raise NotImplementedError(
            "opteryx-iceberg's FileIO serves whole-object metadata reads only; exists() is "
            "not on its read path."
        )

    def open(self, seekable: bool = True) -> InputStream:
        return BytesIO(self._bytes())


class OpteryxFileIO(PyIcebergFileIO):
    """pyiceberg FileIO reading through opteryx-core (see module docstring)."""

    def __init__(self, properties: dict | None = None):
        super().__init__(properties or {})
        self._reader = _ObjectReader(dict(self.properties))

    def new_input(self, location: str) -> OpteryxInputFile:
        return OpteryxInputFile(location, self._reader)

    def new_output(self, location: str) -> PyIcebergOutputFile:
        raise NotImplementedError(
            f"opteryx-iceberg is read-only; refusing to write {location!r}."
        )

    def delete(self, location) -> None:
        raise NotImplementedError("opteryx-iceberg is read-only; refusing to delete.")


# ── opteryx_catalog surface ──────────────────────────────────────────────────


class IcebergInputFile(BaseInputFile):
    def __init__(self, location: str, reader: _ObjectReader):
        super().__init__(location)
        self._reader = reader

    def open(self) -> BinaryIO:
        return BytesIO(self._reader.read(self.location))


class IcebergFileIO(FileIO):
    """opteryx_catalog's FileIO surface over the same whole-object reads."""

    def __init__(self, properties: dict | None = None):
        self._reader = _ObjectReader(dict(properties or {}))

    def new_input(self, location: str) -> IcebergInputFile:
        return IcebergInputFile(location, self._reader)

    def new_output(self, location: str):
        raise NotImplementedError(
            f"opteryx-iceberg is read-only; refusing to write {location!r}."
        )
