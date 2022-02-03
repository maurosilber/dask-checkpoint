import dask

from . import _compatibility  # noqa: F401
from .serializer import serializer
from .storage import Storage
from .task import Task, dataclass, dependency, task

dask.config.set(delayed_pure=True)

__all__ = ["dataclass", "dependency", "Storage", "serializer", "Task", "task"]
