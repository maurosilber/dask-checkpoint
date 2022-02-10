from __future__ import annotations

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

    @contextmanager
    def __call__(self, *, save: bool):
        """A single-use context-manager that preprends
        and then removes a dask optimizer function.
        """
        optimize = self.get_optimizer(save=save)
        optimizations = dask.config.get("optimizations", ())
        with dask.config.set(optimizations=(optimize, *optimizations)):
            yield

    def load(self, task: Task):
        value = self.fs[task.dask_key]
        return task.decode(value)

    def save(self, task: Task, value):
        self.fs[task.dask_key] = task.encode(value)
        return value

    def get_optimizer(self, save: bool):
        def yield_tasks(dsk, keys) -> Iterator[tuple[str, tuple, Task]]:
            """Traverses the dask graph yielding Task instances with Task.save=True."""

            for key, value in dsk.items():
                func = value[0]

                if func is apply:
                    # Called with kwargs
                    func = value[1]

                if isinstance(func, Task) and func.save:
                    yield key, value, func

        def optimize(dsk, keys):
            """Traverses the dask graph checking for Task instances,
            and replaces them with load instructions if they already exists in storage,
            or injects save instructions otherwise (if save=True).
            """

            dsk = dsk.to_dict()

            # iterate over task where task.save is True
            for key, value, task in yield_tasks(dsk, keys):
                # If task is already in storage, replace with a load instruction
                if key in self.fs:
                    dsk[key] = (self.load, task)
                elif save:
                    # If not, inject a save instruction, which saves "value",
                    # which is a tuple representing the computation:
                    #   value = (task, *params)
                    dsk[key] = (self.save, task, value)

            dsk, _ = dask.optimization.cull(dsk, keys)
            return dsk

        return optimize
