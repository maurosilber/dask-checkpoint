from __future__ import annotations

import types
from collections import ChainMap
from contextlib import contextmanager
from typing import Iterator, MutableMapping

import dask
import dask.optimization
import fsspec
from dask.core import literal

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
        try:
            fs = fs.__fspath__()
        except AttributeError:
            pass

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
    def __call__(self, *, save: bool, nested: bool = True):
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

    def load(self, task: Task):
        value = self.fs[task.dask_key]
        return task.decode(value)

    def save(self, task: Task, value):
        self.fs[task.dask_key] = task.encode(value)
        return value

    @staticmethod
    def _yield_tasks(dsk, keys) -> Iterator[tuple[str, tuple, Task]]:
        """Traverses the dask graph yielding Task instances with Task.save=True."""

        for key, value in dsk.items():
            try:
                func = value[1]
            except KeyError:
                continue

            if isinstance(func, literal):
                task = func.data
            else:
                continue

            if isinstance(task, Task) and task.save:
                yield key, value, task

    def optimize_load(self, dsk, keys):
        """Inject load instructions for tasks already in storage."""
        dsk = dict(dsk)

        # iterate over task where task.save is True
        for key, _, task in self._yield_tasks(dsk, keys):
            # If task is already in storage, replace with a load instruction
            if key in self.fs:
                dsk[key] = (self.load, task)

        dsk, _ = dask.optimization.cull(dsk, keys)
        return dsk

    def optimize_save(self, dsk, keys):
        """Inject save instructions for tasks."""
        dsk = dict(dsk)

        # iterate over task where task.save is True
        for key, value, task in self._yield_tasks(dsk, keys):
            # Inject a save instruction, which saves "value",
            # which is a tuple representing the computation:
            #   value = (task, *params)
            dsk[key] = (self.save, task, value)

        return dsk
