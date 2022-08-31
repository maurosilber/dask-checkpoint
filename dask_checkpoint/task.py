from __future__ import annotations

from collections.abc import Callable
from inspect import signature
from typing import Generic, TypeVar

try:
    from typing import ParamSpec
except ImportError:
    from typing_extensions import ParamSpec

from dask import delayed

from .encoder import DefaultEncoder, Encoder
from .hasher import ArgumentHasher, FunctionHasher, function_name, tokenize

T = TypeVar("T")
P = ParamSpec("P")


class task(Generic[P, T]):
    """Build a task from a callable. Can be used as a decorator."""

    def __init__(
        self,
        func: Callable[P, T] = None,
        *,
        save: bool = False,
        name: str | FunctionHasher = function_name,
        hasher: ArgumentHasher = tokenize,
        encoder: Encoder[T, bytes] = DefaultEncoder(),
    ):
        if not isinstance(name, str):
            name = name(func)

        self.func = func
        self.save = save
        self.name = name
        self.hasher = hasher
        self.encoder = encoder

        self.__signature__ = signature(func)
        self.delayed_func = delayed(Task(self), name=self.name, pure=True)

    def __new__(cls, func=None, **kwargs):
        if func is None:
            # Passing kwargs before applying as a decorator
            def task_partial(func):
                return cls(func, **kwargs)

            return task_partial

        return super().__new__(cls)

    def key(self, kwargs):
        h = self.hasher(kwargs)
        return f"{self.name}{h}"

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        bound = self.__signature__.bind(*args, **kwargs)
        key = self.key(bound.arguments)
        return self.delayed_func(*args, **kwargs, dask_key_name=key)

    def __repr__(self):
        if self.save:
            return f"task({self.func}, save={self.save}, encoder={self.encoder})"
        else:
            return f"task({self.func}, save={self.save})"


class Task:
    __slots__ = ("task",)

    def __init__(self, task: task):
        self.task = task

    @property
    def save(self):
        return self.task.save

    def encode(self, value):
        return self.task.encoder.encode(value)

    def decode(self, value):
        return self.task.encoder.decode(value)

    def __call__(self, *args, **kwds):
        return self.task.func(*args, **kwds)
