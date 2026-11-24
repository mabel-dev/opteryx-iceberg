"""Iceberg metadata reads through opteryx-core's own storage clients.

Two FileIO surfaces share ONE read path:

- `OpteryxFileIO` is a pyiceberg FileIO. IcebergMetastore installs it as the
  catalog's `py-io-impl`, so a catalog that reads `metadata.json` itself
  (SQL/Hive/Glue) reads it through this, and `opteryx_iceberg.manifests`
  fetches the manifest list and manifests through one built per table from
  the table's merged properties - never through `table.io`, which a REST
  catalog can re-point by serving its own `py-io-impl`.
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

Writes (Tier 2: CREATE TABLE / INSERT) go through the same two surfaces:
local files directly, S3 and GCS through opteryx_catalog's own FileIO
(opteryx-core's filesystems are read-only) built with opteryx-core's S3
endpoint and credentials, so a write runs under the identity reads do. The
same identity check refuses a write the properties expect elsewhere.

Because every read and write runs as Opteryx, a location is opened with
Opteryx's access, not with the access of whoever wrote the catalog. A TENANT's
catalog (IcebergMetastore) is therefore held to `TenantLocations`: gs:// and
s3:// objects only, and none in a bucket the deployment reserves - otherwise a
workspace's catalog could name the worker's own disk, or another workspace's
native data, and have Opteryx read it for them. `TenantFileIO` is the FileIO
that carries it; an operator's catalog (UnrestrictedIcebergMetastore) uses
`OpteryxFileIO`, which admits every location the reader supports.
"""

from __future__ import annotations

import os
import re
import threading
from io import BytesIO
from typing import BinaryIO

from opteryx.connectors.io_systems import create_filesystem
from opteryx_catalog.iops.base import FileIO
from opteryx_catalog.iops.base import InputFile as BaseInputFile
from opteryx_catalog.iops.base import OutputFile as BaseOutputFile
from pyiceberg.io import FileIO as PyIcebergFileIO
from pyiceberg.io import InputFile as PyIcebergInputFile
from pyiceberg.io import InputStream
from pyiceberg.io import OutputFile as PyIcebergOutputFile

from opteryx_iceberg.dataset import _reader_path

# pyiceberg's `py-io-impl` values: OpteryxFileIO for an operator's catalog,
# TenantFileIO for a tenant's - see IcebergMetastore.
PY_IO_IMPL = "opteryx_iceberg.fileio.OpteryxFileIO"
TENANT_PY_IO_IMPL = "opteryx_iceberg.fileio.TenantFileIO"

# The buckets this deployment owns - its native catalog's data, query results,
# scratch: anything Opteryx's identity can read that is not a customer's lake -
# as a comma-separated list of `gs://<bucket>` / `s3://<bucket>`. Required for a
# tenant's catalog to be read at all: an unset list would mean no bucket is
# protected, so it is refused rather than assumed empty.
RESERVED_BUCKETS_ENV = "OPTERYX_ICEBERG_RESERVED_BUCKETS"

# A bucket name as GCS and S3 accept it: lowercase letters, digits, dots, dashes
# and underscores, alphanumeric at both ends. Anything else - a capital, a
# percent-escape, a userinfo `@`, an empty name - is refused, never normalised,
# so the name compared against the reserved list is the name the store resolves.
_BUCKET_NAME = re.compile(r"[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]")

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


# ── which locations a catalog may name ───────────────────────────────────────


def _object_bucket(location: str) -> tuple[str, str] | None:
    """(protocol, bucket) for a gs:// or s3:// location, None for any other.
    Raises ValueError for an object location whose bucket is not a valid name."""
    scheme, separator, remainder = location.partition("://")
    protocol = _PROTOCOL_BY_SCHEME.get(scheme) if separator else None
    if protocol not in ("gs", "s3"):
        return None
    bucket = remainder.partition("/")[0]
    if not _BUCKET_NAME.fullmatch(bucket):
        raise ValueError(f"{location!r} does not name a valid {protocol}:// bucket")
    return protocol, bucket


def _reserved_buckets() -> frozenset:
    """The deployment's reserved buckets, as (protocol, bucket), from RESERVED_BUCKETS_ENV."""
    if RESERVED_BUCKETS_ENV not in os.environ:
        raise RuntimeError(
            f"{RESERVED_BUCKETS_ENV} is not set. A workspace's Iceberg catalog is read with "
            "Opteryx's own identity, so it cannot be read until the deployment names the "
            "buckets that identity must not be pointed at (comma-separated gs://<bucket> / "
            "s3://<bucket>)."
        )
    reserved = set()
    for entry in os.environ[RESERVED_BUCKETS_ENV].split(","):
        entry = entry.strip()
        scheme, separator, bucket = entry.partition("://")
        if (
            not separator
            or _PROTOCOL_BY_SCHEME.get(scheme) not in ("gs", "s3")
            or not _BUCKET_NAME.fullmatch(bucket)
        ):
            raise RuntimeError(
                f"{RESERVED_BUCKETS_ENV}: {entry!r} is not gs://<bucket> or s3://<bucket>."
            )
        reserved.add((_PROTOCOL_BY_SCHEME[scheme], bucket))
    return frozenset(reserved)


class AnyLocation:
    """An OPERATOR's catalog may name every location the reader supports."""

    def check(self, location: str) -> None:
        return None


class TenantLocations:
    """A TENANT's catalog may name gs:// and s3:// objects outside the
    deployment's reserved buckets, and nothing else.

    Every location is opened as Opteryx (see the module docstring). A local path
    would be read from the worker's own disk, and a reserved bucket holds data
    that belongs to other workspaces or to Opteryx - both readable by Opteryx's
    identity, neither the tenant's. A stopgap: a customer bucket granted to
    Opteryx's shared identity is still reachable from any tenant's catalog until
    each workspace reads under an identity of its own.
    """

    def __init__(self):
        self._reserved: frozenset | None = None

    def check(self, location: str) -> None:
        bucket = _object_bucket(location)
        if bucket is None:
            raise PermissionError(
                f"opteryx-iceberg refuses {location!r}: a workspace's catalog may only name "
                "gs:// or s3:// locations."
            )
        if self._reserved is None:
            self._reserved = _reserved_buckets()
        if bucket in self._reserved:
            raise PermissionError(
                f"opteryx-iceberg refuses {location!r}: that bucket belongs to this "
                "deployment, not to the workspace's catalog."
            )


class _ObjectReader:
    """Whole-object reads for one set of Iceberg properties, of the locations
    `policy` admits."""

    def __init__(self, properties: dict, policy):
        self._properties = properties
        self.policy = policy
        self._checked: set = set()

    def read(self, location: str) -> bytes:
        self.policy.check(location)
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


class _LocalOutputStream:
    """A local file written to a sibling temp path and renamed into place on
    close, so a reader never sees a partial file; abort removes the temp."""

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._path = path
        self._temp = f"{path}.{os.getpid()}.{threading.get_ident()}.tmp"
        self._handle = open(self._temp, "wb")

    def write(self, data) -> int:
        return self._handle.write(data)

    def close(self) -> None:
        if self._handle.closed:
            return
        self._handle.close()
        os.replace(self._temp, self._path)

    def abort(self) -> None:
        if not self._handle.closed:
            self._handle.close()
        os.unlink(self._temp)


_catalog_ios: dict = {}
_catalog_ios_lock = threading.Lock()


def _catalog_io(protocol: str):
    """The process's one opteryx_catalog FileIO that writes `protocol`, under
    the identity opteryx-core reads it with: opteryx-core's filesystems are
    read-only, and the catalog's own FileIO is what the native catalog writes
    through."""
    with _catalog_ios_lock:
        io = _catalog_ios.get(protocol)
        if io is not None:
            return io
        if protocol == "s3":
            try:
                from botocore.config import Config
            except ModuleNotFoundError as err:
                raise ModuleNotFoundError(
                    "Writing Iceberg tables to S3 needs boto3: "
                    "pip install 'opteryx-iceberg[s3]'"
                ) from err
            from opteryx_catalog.iops.s3 import S3FileIO

            filesystem = _filesystem("s3")
            access_key, secret_key, token = filesystem.credentials.frozen()
            # shortcut: credentials are frozen when the client is built; a
            # rotating session token outlives no process restart - resolve per
            # request if a long-lived writer needs it.
            io = S3FileIO(
                endpoint_url=filesystem.endpoint or None,
                region_name=filesystem.region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=token,
                config=Config(s3={"addressing_style": "path" if filesystem.endpoint else "auto"}),
            )
        elif protocol == "gs":
            from opteryx_catalog.iops.gcs import GcsFileIO

            io = GcsFileIO()
        else:
            raise ValueError(f"opteryx-iceberg has no catalog FileIO for protocol {protocol!r}")
        _catalog_ios[protocol] = io
        return io


class _ObjectWriter:
    """Writes, existence checks and deletes for one set of Iceberg properties,
    refused under the same identity check and location policy as reads."""

    def __init__(self, properties: dict, policy):
        self.properties = properties
        self.policy = policy
        self._checked: set = set()

    def _target(self, location: str) -> tuple[str, str]:
        self.policy.check(location)
        protocol, path = _resolve(location)
        if protocol != "file" and protocol not in self._checked:
            _check_identity(protocol, _filesystem(protocol), self.properties)
            self._checked.add(protocol)
        return protocol, path

    def open(self, location: str):
        """A stream with write / close / abort; nothing is visible until close."""
        protocol, path = self._target(location)
        if protocol == "file":
            return _LocalOutputStream(path)
        return _catalog_io(protocol).new_output(path).create()

    def exists(self, location: str) -> bool:
        protocol, path = self._target(location)
        if protocol == "file":
            return os.path.exists(path)
        return _catalog_io(protocol).exists(path)

    def delete(self, location: str) -> None:
        protocol, path = self._target(location)
        if protocol == "file":
            os.unlink(path)
        else:
            _catalog_io(protocol).delete(path)


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
    """pyiceberg FileIO reading through opteryx-core (see module docstring), for
    an operator's catalog: every location the reader supports."""

    def __init__(self, properties: dict | None = None):
        super().__init__(properties or {})
        self.policy = self.new_policy()
        self._reader = _ObjectReader(dict(self.properties), self.policy)

    @staticmethod
    def new_policy():
        return AnyLocation()

    def new_input(self, location: str) -> OpteryxInputFile:
        return OpteryxInputFile(location, self._reader)

    def new_output(self, location: str) -> PyIcebergOutputFile:
        return OpteryxOutputFile(location, _ObjectWriter(dict(self.properties), self.policy))

    def delete(self, location: str) -> None:
        _ObjectWriter(dict(self.properties), self.policy).delete(location)


class TenantFileIO(OpteryxFileIO):
    """OpteryxFileIO for a tenant's catalog: `TenantLocations` only."""

    @staticmethod
    def new_policy():
        return TenantLocations()


class _PyIcebergOutputStream:
    """pyiceberg's OutputStream protocol over one catalog/local write stream."""

    def __init__(self, stream):
        self._stream = stream

    def write(self, data: bytes) -> int:
        return self._stream.write(data)

    def close(self) -> None:
        self._stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self._stream.close()
        else:
            self._stream.abort()


class OpteryxOutputFile(PyIcebergOutputFile):
    """An Iceberg metadata file pyiceberg writes (a SQL/Hive/Glue catalog's
    metadata.json), through the same writer as data files and manifests."""

    def __init__(self, location: str, writer: "_ObjectWriter"):
        super().__init__(location)
        self._writer = writer

    def __len__(self) -> int:
        raise NotImplementedError("opteryx-iceberg output files do not report a length.")

    def exists(self) -> bool:
        return self._writer.exists(self.location)

    def to_input_file(self) -> OpteryxInputFile:
        return OpteryxInputFile(
            self.location, _ObjectReader(self._writer.properties, self._writer.policy)
        )

    def create(self, overwrite: bool = False) -> _PyIcebergOutputStream:
        if not overwrite and self._writer.exists(self.location):
            raise FileExistsError(f"refusing to overwrite {self.location!r}")
        return _PyIcebergOutputStream(self._writer.open(self.location))


# ── opteryx_catalog surface ──────────────────────────────────────────────────


class IcebergInputFile(BaseInputFile):
    def __init__(self, location: str, reader: _ObjectReader):
        super().__init__(location)
        self._reader = reader

    def open(self) -> BinaryIO:
        return BytesIO(self._reader.read(self.location))


class IcebergFileIO(FileIO):
    """opteryx_catalog's FileIO surface over the same whole-object reads, of the
    locations `policy` admits (`AnyLocation` / `TenantLocations`)."""

    def __init__(self, properties: dict | None, policy):
        self._reader = _ObjectReader(dict(properties or {}), policy)
        self._writer = _ObjectWriter(dict(properties or {}), policy)

    def new_input(self, location: str) -> IcebergInputFile:
        return IcebergInputFile(location, self._reader)

    def new_output(self, location: str) -> "IcebergOutputFile":
        return IcebergOutputFile(location, self._writer)

    def delete(self, location: str) -> None:
        self._writer.delete(location)


class IcebergOutputFile(BaseOutputFile):
    def __init__(self, location: str, writer: _ObjectWriter):
        super().__init__(location)
        self._writer = writer

    def create(self):
        return self._writer.open(self.location)
