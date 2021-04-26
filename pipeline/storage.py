import abc
import pathlib
from contextvars import ContextVar

import fsspec

storage_ctx = ContextVar("storage_ctx")


class Storage(abc.ABC):
    @abc.abstractmethod
    def exists(self, key: str) -> bool:
        return NotImplementedError

    @abc.abstractmethod
    def read(self, key: str):
        return NotImplementedError

    @abc.abstractmethod
    def write(self, key: str):
        return NotImplementedError

    def __enter__(self):
        self.token = storage_ctx.set(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        storage_ctx.reset(self.token)


class FSStorage(Storage):
    """Filesystem storage provided by fsspec."""

    def __init__(self, uri: str, **options):
        file = fsspec.open(uri, **options)
        self.fs: fsspec.AbstractFileSystem = file.fs
        self.base_path = pathlib.Path(file.path)
        self.options = options

    def path(self, key):
        return str(self.base_path / key)

    def exists(self, key: str) -> bool:
        return self.fs.exists(self.path(key))

    def read(self, key: str):
        return fsspec.core.OpenFile(self.fs, self.path(key), mode="rb")

    def write(self, key: str):
        return fsspec.core.OpenFile(self.fs, self.path(key), mode="wb")
