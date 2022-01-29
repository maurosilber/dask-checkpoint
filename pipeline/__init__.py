import dask

from .storage import Storage
from .task import Task, dataclass, dependency, task

dask.config.set(delayed_pure=True)

__all__ = ["dataclass", "dependency", "Storage", "Task", "task"]
