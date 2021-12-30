from __future__ import annotations

from contextlib import contextmanager

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
    fs : fsspec.FSMap | str
        If it is a str, it constructs a FSMap with fsspec.get_mapper.
    """

    def __init__(self, fs: fsspec.FSMap | str, **get_mapper_kwargs):
        if isinstance(fs, str):
            fs = fsspec.get_mapper(fs, **get_mapper_kwargs)
        self.fs = fs

    def __call__(self, *, save: bool):
        return set_optimize_func(self.get_optimizer(save=save))

    def load(self, task: Task):
        value = self.fs[task.dask_key]
        return task.decode(value)

    def save(self, task: Task, value):
        self.fs[task.dask_key] = task.encode(value)
        return value

    def get_optimizer(self, save: bool):
        def optimize(dsk, keys):
            """Traverses the dask graph checking for Task instances,
            and replaces them with load instructions if they already exists in storage,
            or injects save instructions otherwise (if save=True).
            """

            new_dsk = dsk.to_dict()

            for key, value in new_dsk.items():
                # value is a tuple representing the computation:
                #   value = (func, *params)
                #
                # If func is a task (with save=True), check if it is already in storage
                # If:
                # - True: replace it with a load instruction, discarding "value"
                # - False: inject a save instruction, which saves "value"

                task = value[0]
                if task is apply:
                    # Called with kwargs
                    task = value[1]

                if not isinstance(task, Task) or not task.save:
                    continue

                if key in self.fs:
                    new_dsk[key] = (self.load, task)
                elif save:
                    new_dsk[key] = (self.save, task, value)

            new_dsk, _ = dask.optimization.cull(new_dsk, keys)
            return new_dsk

        return optimize


@contextmanager
def set_optimize_func(optimize):
    """A single-use context-manager that preprends
    and then removes a dask optimizer function.
    """
    optimizations = dask.config.get("optimizations", ())
    dask.config.set(optimizations=(optimize, *optimizations))
    yield
    dask.config.set(optimizations=optimizations)
