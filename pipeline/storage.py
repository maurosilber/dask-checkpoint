from __future__ import annotations

from collections import ChainMap
from contextlib import contextmanager
from typing import Iterator, MutableMapping

import dask
import dask.optimization
import fsspec
from dask.utils import apply

from .task import Task


class Storage:
    """Saves and loads task results to the given fsspec.FSMap instance.

    Calling an storage instance returns a single-use context manager,
    inside which dask graphs are injected with load and save functions.

    >>> storage = Storage("memory://")  # a dict-backed in-memory storage.

    >>> with storage(save=True):
            task.compute()  # task is loaded (if it exists) or saved

    >>> with storage(save=False):
            task.compute()  # task is only loaded (if it exists)

    Parameters
    ----------
    fs : MutableMapping[str, bytes] | str
        If it is a str, it constructs a FSMap with fsspec.get_mapper.
    """

    fs: MutableMapping[str, bytes]

    def __init__(self, fs: MutableMapping[str, bytes] | str, **get_mapper_kwargs):
        if isinstance(fs, str):
            fs = fsspec.get_mapper(fs, **get_mapper_kwargs)
        self.fs = fs

    @classmethod
    def from_chain(cls, *storages: Storage) -> Storage:
        """Create a Storage by chaining multiple Storages with a collections.ChainMap.

        It will only save to the first Storage.
        """
        return Storage(ChainMap(*[s.fs for s in storages]))

    @contextmanager
    def __call__(self, *, save: bool):
        """A single-use context-manager that preprends
        and then removes dask optimizers function.
        """
        optimizers = self.get_optimizers(save=save)
        optimizations = dask.config.get("optimizations", ())
        with dask.config.set(optimizations=(*optimizers, *optimizations)):
            yield

    def load(self, task: Task):
        value = self.fs[task.dask_key]
        return task.decode(value)

    def save(self, task: Task, value):
        self.fs[task.dask_key] = task.encode(value)
        return value

    def get_optimizers(self, save: bool) -> tuple[callable]:
        def yield_tasks(dsk, keys) -> Iterator[tuple[str, tuple, Task]]:
            """Traverses the dask graph yielding Task instances with Task.save=True."""

            for key, value in dsk.items():
                func = value[0]

                if func is apply:
                    # Called with kwargs
                    func = value[1]

                if isinstance(func, Task) and func.save:
                    yield key, value, func

        def optimize_load(dsk, keys):
            """Inject load instructions for tasks already in storage."""
            dsk = dict(dsk)

            # iterate over task where task.save is True
            for key, _, task in yield_tasks(dsk, keys):
                # If task is already in storage, replace with a load instruction
                if key in self.fs:
                    dsk[key] = (self.load, task)

            dsk, _ = dask.optimization.cull(dsk, keys)
            return dsk

        def optimize_save(dsk, keys):
            """Inject save instructions for tasks."""
            dsk = dict(dsk)

            # iterate over task where task.save is True
            for key, value, task in yield_tasks(dsk, keys):
                # Inject a save instruction, which saves "value",
                # which is a tuple representing the computation:
                #   value = (task, *params)
                dsk[key] = (self.save, task, value)

            return dsk

        if save:
            return (optimize_load, optimize_save)
        else:
            return (optimize_load,)
