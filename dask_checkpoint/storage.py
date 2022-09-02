from __future__ import annotations

import types
from collections import ChainMap
from contextlib import contextmanager
from typing import Iterator, MutableMapping

import dask
import dask.optimization
import fsspec
from dask.core import literal
from dask.delayed import Delayed, apply

from .task import Task


class Storage:
    """Saves and loads task results to a MutableMapping.

    Calling an storage instance returns a single-use context manager,
    inside which dask graphs are injected with load and save functions.

    >>> from dask_checkpoint import task, Storage
    >>> storage = Storage({})  # a dict-backed storage.
    >>> @task
    ... def func():
    ...     return 42
    >>> with storage(save=True):
    ...     func().compute()  # task is loaded (if it exists) or saved
    42
    >>> with storage(save=False):
    ...     func().compute()  # task is only loaded (if it exists)
    42
    """

    data: MutableMapping[str, bytes]

    def __init__(self, data: MutableMapping[str, bytes]):
        self.data = data

    @classmethod
    def from_fsspec(cls, path: str, **kwargs):
        data = fsspec.get_mapper(path, **kwargs)
        return cls(data)

    @classmethod
    def from_chain(cls, *storages: Storage) -> Storage:
        """Create a Storage by chaining multiple Storages with a collections.ChainMap.

        It will only save to the first Storage.
        """
        return cls(ChainMap(*(s.data for s in storages)))

    @contextmanager
    def __call__(self, *, save: bool = True, nested: bool = True):
        """A single-use context-manager that to set and then restore
        dask optimizers function.

        Parameters
        ----------
        save : bool
            If False, adds load instructions.
            If True, adds load and save instructions.
        nested : bool, optional. Default: True
            If False, ignores previous Storage contexts.
        """
        if not nested:
            if save:
                optimizations = (self.optimize_load, self.optimize_save)
            else:
                optimizations = (self.optimize_load,)
        else:
            # Get the current optimization functions.
            optimizations = dask.config.get("optimizations", ())

            if not save:
                optimizations = (self.optimize_load, *optimizations)
            else:
                # When save=True, we do not want to insert our save optimizer
                # before load optimizers from previous Storages!
                # We look for the first Storage.optimize_save in optimizations
                for i, meth in enumerate(optimizations):
                    if (
                        isinstance(meth, types.MethodType)
                        and meth.__func__ is Storage.optimize_save
                    ):
                        loads, saves = optimizations[:i], optimizations[i:]
                        optimizations = (
                            self.optimize_load,
                            *loads,
                            self.optimize_save,
                            *saves,
                        )
                        break
                else:
                    # If we didn't find any, we assume that they are all load.
                    optimizations = (
                        self.optimize_load,
                        *optimizations,
                        self.optimize_save,
                    )

        with dask.config.set(optimizations=optimizations):
            yield

    def load(self, key: literal[str], task: Task):
        value = self.data[key.data]
        return task.decode(value)

    def save(self, key: literal[str], task: Task, value):
        self.data[key.data] = task.encode(value)
        return value

    def __contains__(self, task: Task) -> bool:
        if isinstance(task, Delayed):
            return task.key in self

        raise NotImplementedError

    def optimize_load(self, dsk, keys):
        """Inject load instructions for tasks already in storage."""
        dsk = dict(dsk)

        # iterate over task where task.save is True
        for key, _, task in _yield_tasks(dsk, keys):
            # If task is already in storage, replace with a load instruction
            if key in self.data:
                dsk[key] = (self.load, literal(key), task)

        dsk, _ = dask.optimization.cull(dsk, keys)
        return dsk

    def optimize_save(self, dsk, keys):
        """Inject save instructions for tasks."""
        dsk = dict(dsk)

        # iterate over task where task.save is True
        for key, value, task in _yield_tasks(dsk, keys):
            # Inject a save instruction, which saves "value",
            # which is a tuple representing the computation:
            #   value = (task, *params)
            dsk[key] = (self.save, literal(key), task, value)

        return dsk


def _yield_tasks(dsk, keys) -> Iterator[tuple[str, tuple, Task]]:
    """Traverses the dask graph yielding Task instances with Task.save=True."""

    for key, value in dsk.items():
        task = value[0]
        if task is apply:
            task = value[1]

        if isinstance(task, Task):
            if task.save:
                yield key, value, task
