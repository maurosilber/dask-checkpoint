from __future__ import annotations

from functools import cached_property
from inspect import Parameter, Signature, signature
from types import FunctionType
from typing import TypeVar

import dask
from dask.base import tokenize
from dask.optimization import cull
from dask.utils import ensure_dict
from typing_extensions import Annotated

from .serializer import Serializer
from .storage import Storage, storage_ctx


def _optimize_no_storage(dsk, keys):
    """Replace Task instances with Task.run function,
    and remove unnecesary tasks not required to calculate keys.
    """
    dsk, _ = cull(dsk, keys)
    for k, v in dsk.items():
        task = v[0]
        if isinstance(task, Task):
            dsk[k] = (task.run, *v[1:])
    return dsk


def _optimize_with_storage(dsk, keys, storage):
    """Replace Task instances with either:
        - Task.run function
        - Load function
        - Task.run & Save functions
    and remove unnecesary tasks not required to calculate keys.
    """
    dsk, _ = cull(dsk, keys)

    for k, v in dsk.items():
        task = v[0]

        if not isinstance(task, Task):
            continue

        if not task._save:
            dsk[k] = (task.run, *v[1:])
            continue

        if storage.exists(task.key):
            dsk[k] = (task._build_load_func(), storage.read(task.key))
        else:
            # TODO: What if the scheduler runs the task more than once?
            dsk[k] = (
                task._build_dump_func(),
                storage.write(task.key),
                (task.run, *v[1:]),
            )

    # Remove unnecesary dependencies replaced by loads
    dsk, _ = cull(dsk, keys)
    return dsk


def _optimize(dsk, keys):
    """Replace Task instances with Task.run, loading,
    or Task.run and saving functions, and remove
    unnecesary tasks not required to calculate keys.
    """
    dsk = ensure_dict(dsk)
    storage: Storage = storage_ctx.get(None)
    if storage is None:
        dsk = _optimize_no_storage(dsk, keys)
    else:
        dsk = _optimize_with_storage(dsk, keys, storage)
    return dsk


dask.config.set(delayed_optimize=_optimize)


class MetaDependency(type):
    def __repr__(self):
        return "dependency"


class dependency(metaclass=MetaDependency):
    """Dependency descriptor."""

    def __init__(self, dep=None):
        self.dep = dep
        self.is_func = isinstance(dep, FunctionType)
        if self.is_func:
            self.__doc__ = dep.__doc__

    def __get__(self, task, objtype=None):
        if task is None:
            return self

        if self.is_func:
            return self.dep(task)
        else:
            return self.dep

    def __class_getitem__(cls, item):
        return DependencyType[item]


DependencyType = Annotated[TypeVar("T"), dependency]  # noqa: F821


class Save:
    serializers: tuple[Serializer, ...] = ()

    @staticmethod
    def dumps(x):
        return x

    @staticmethod
    def loads(x):
        return x

    @classmethod
    def _build_dump_func(cls):
        serializers = (cls.dumps, *(s.dumps for s in cls.serializers))

        def _dump(stream, result):
            out = result
            for func in serializers:
                out = func(out)

            with stream as s:
                s.write(out)

            return result

        return _dump

    @classmethod
    def _build_load_func(cls):
        serializers = (cls.loads, *(s.loads for s in cls.serializers))

        def _load(stream):
            with stream as s:
                result = s.read()

            for func in serializers[::-1]:
                result = func(result)

            return result

        return _load


class Task(Save):
    def __init_subclass__(cls, save=False):
        """Check run and build task signatures.

        Run signature must not have keyword-only parameters.

        Task signature:
            Run method -> positional parameters
            Dependencies -> keyword-only parameters
        """

        # Check run signature
        run_params = signature(cls.run).parameters
        kinds = (Parameter.KEYWORD_ONLY, Parameter.VAR_KEYWORD)
        if any(p.kind in kinds for p in run_params.values()):
            raise Exception("run parameters must be positional, not keyword-only.")

        # Build task signature
        task_params = run_params.copy()

        for name in cls._dependencies():
            if name not in task_params:
                task_params[name] = Parameter(name, Parameter.KEYWORD_ONLY)

        cls.__signature__ = Signature(task_params.values())
        cls._save = save

    @classmethod
    def _dependencies(cls):
        return (d for d in dir(cls) if isinstance(getattr(cls, d), dependency))

    @staticmethod
    def run():
        """Compute the result.

        Run signature must have positional parameters, which are
        declared as class attributes or dependency methods.
        """
        raise NotImplementedError

    @cached_property
    def key(self) -> str:
        name = self.__class__.__qualname__
        return f"{name}-{tokenize(*self._run_args)}"

    def __new__(cls, *args, _delayed=True, **kwargs):
        # Create instance
        self = super().__new__(cls)

        # Initialize instance attributes.
        self._bound = cls.__signature__.bind_partial(*args, **kwargs)
        for name, value in self._bound.arguments.items():
            setattr(self, name, value)

        if _delayed:
            return dask.delayed(self, traverse=False)(
                *self._run_args, dask_key_name=self.key
            )
        else:
            # Return instance. Needed for Task serialization.
            return self

    def __getnewargs_ex__(self):
        return self._bound.args, {**self._bound.kwargs, "_delayed": False}

    @cached_property
    def _run_args(self):
        parameters = signature(self.run).parameters
        return tuple(getattr(self, k) for k in parameters)

    def __call__(self, *args, **kwargs):
        # Needed for dask to consider instances as (dask) tasks.
        raise NotImplementedError
