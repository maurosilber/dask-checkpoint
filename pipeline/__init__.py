import dask

from .task import Task, dependency

dask.config.set(delayed_pure=True)

__all__ = ["Task", "dependency"]
