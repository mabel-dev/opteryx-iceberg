"""FileIO adapter: opteryx_catalog.iops.base.FileIO <-> pyiceberg.io.pyarrow.PyArrowFileIO.

PyArrowFileIO already handles local disk, S3, GCS, and Azure based on the
location's scheme, so this is a thin signature adapter, not a new backend.
"""

from __future__ import annotations

from typing import BinaryIO

from opteryx_catalog.iops.base import FileIO
from opteryx_catalog.iops.base import InputFile as BaseInputFile
from opteryx_catalog.iops.base import OutputFile as BaseOutputFile
from pyiceberg.io.pyarrow import PyArrowFileIO


class IcebergInputFile(BaseInputFile):
    def __init__(self, location: str, pyarrow_io: PyArrowFileIO):
        super().__init__(location)
        self._pyarrow_io = pyarrow_io

    def open(self) -> BinaryIO:
        return self._pyarrow_io.new_input(self.location).open()


class IcebergOutputFile(BaseOutputFile):
    def __init__(self, location: str, pyarrow_io: PyArrowFileIO):
        super().__init__(location)
        self._pyarrow_io = pyarrow_io

    def create(self):
        return self._pyarrow_io.new_output(self.location).create()


class IcebergFileIO(FileIO):
    """Adapts pyiceberg's PyArrowFileIO to opteryx_catalog's FileIO surface."""

    def __init__(self, properties: dict | None = None):
        self._pyarrow_io = PyArrowFileIO(properties or {})

    def new_input(self, location: str) -> IcebergInputFile:
        return IcebergInputFile(location, self._pyarrow_io)

    def new_output(self, location: str) -> IcebergOutputFile:
        return IcebergOutputFile(location, self._pyarrow_io)
