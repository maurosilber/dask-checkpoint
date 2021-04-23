import abc
import pathlib
from contextvars import ContextVar
from typing import Union

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

    def __init__(self, path: Union[str, pathlib.Path] = "", protocol: str = "file"):
        self.fs: fsspec.AbstractFileSystem = fsspec.filesystem(protocol)
        self.base_path = pathlib.Path(path)

    def path(self, key):
        return str(self.base_path / key)

    def exists(self, key: str) -> bool:
        return self.fs.exists(self.path(key))

    def read(self, key: str):
        return self.fs.open(self.path(key), mode="rb")

    def write(self, key: str):
        return self.fs.open(self.path(key), mode="wb")
