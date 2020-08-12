from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class Target:
    """Base target class."""

    def __init_subclass__(cls) -> None:
        assert isinstance(
            cls.complete, property
        ), f"{cls.__qualname__}.complete must be a property."

    @property
    def complete(self):
        return False

    def load(self):
        raise NotImplementedError

    def save(self, result):
        raise NotImplementedError

    def remove(self):
        raise NotImplementedError


@dataclass
class DictTarget(Target):
    """Dict-based storage.

    Doesn't work with process-based or distributed schedulers.
    """

    key: str
    __memory = {}

    @property
    def complete(self):
        return self.key in self.__memory

    def load(self):
        return self.__memory[self.key]

    def save(self, result):
        self.__memory[self.key] = result

    def remove(self):
        del self.__memory[self.key]


@dataclass
class LocalTarget(Target):
    """Base class for file-based targets."""

    path: Path = None
    create_parents: bool = True
    extension: str = ""

    def __post_init__(self):
        self.path = self.path.with_suffix(self.extension)

    def make_parents(self):
        self.path.parent.mkdir(parents=self.create_parents, exist_ok=True)

    @property
    def complete(self):
        return self.path.exists()

    def remove(self):
        return self.path.unlink()
