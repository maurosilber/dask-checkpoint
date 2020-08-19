from contextlib import suppress
from typing import Optional

from dask import config
from dask.callbacks import Callback
from dask.optimization import cull
from dask.utils import ensure_dict

from .task import Task


def get_task_instance(dsk, key) -> Optional[Task]:
    func = dsk[key][0]
    with suppress(AttributeError, NotImplementedError):
        task = func.__self__
        if isinstance(task, Task):
            task.target
            return task


class Save(Callback):
    def _posttask(self, key, result, dsk, state, worker_id):
        task = get_task_instance(dsk, key)
        if task is None:
            return
        if not task.target.complete:
            task.save(result)


class Load:
    def __enter__(self):
        return self.register()

    def __exit__(self, *args):
        self.config.__exit__(*args)

    def register(self):
        self.config = config.set(delayed_optimize=self._optimize)
        return self

    def unregister(self):
        self.__exit__(None, None, None)

    @staticmethod
    def _optimize(dsk, keys):
        dsk = ensure_dict(dsk)
        # TODO: Optimize. No need to check all tasks.
        for key in dsk:
            task = get_task_instance(dsk, key)
            if task is not None and task.target.complete:
                dsk[key] = (task.load,)
        dsk, _ = cull(dsk, keys)
        return dsk
