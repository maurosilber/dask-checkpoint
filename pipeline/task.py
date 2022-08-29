from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Generic, Optional, ParamSpec, TypeVar

import cloudpickle
import zstandard
from dask import delayed

from .encoder import Compressor, Encoder, Encrypter, Serializer
from .hasher import function_name, tokenize

T = TypeVar("T")
P = ParamSpec("P")

FunctionHasher: Callable[[Callable], str]
ArgumentHasher: Callable[[tuple, dict], str]


@dataclass
class task(Generic[P, T]):
    func: Callable[P, T]
    save: bool = False
    name: str | FunctionHasher = function_name
    hasher: ArgumentHasher = tokenize
    encoders: tuple[Encoder] = ()
    serializer: Optional[Serializer] = cloudpickle
    compressor: Optional[Compressor] = zstandard
    encrypter: Optional[Encrypter] = None
    """Build a task from a callable. Can be used as a decorator."""

    def __new__(cls, func=None, **kwargs):
        if func is None:
            # Passing kwargs before applying as a decorator
            def task_partial(func):
                return cls(func, **kwargs)

            return task_partial

        return super().__new__(cls)

    def __post_init__(self):
        if not isinstance(self.name, str):
            self.name = self.name(self.func)
        self.delayed_func = delayed(Task(self), name=self.name, pure=True)

    def key(self, *args, **kwargs):
        h = self.hasher(args, kwargs)
        return f"{self.name}/{h}"

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        key = self.key(*args, **kwargs)
        return self.delayed_func(*args, **kwargs, dask_key_name=key)


class Task:
    __slots__ = ("task",)

    def __init__(self, task: task):
        self.task = task

    @property
    def save(self):
        return self.task.save

    def encode(self, value):
        for encoder in self.task.encoders:
            value = encoder.encode(value)
        serializer = self.task.serializer
        if serializer is not None:
            value = serializer.dumps(value)
        compressor = self.task.compressor
        if compressor is not None:
            value = compressor.compress(value)
        encrypter = self.task.encrypter
        if encrypter is not None:
            value = encrypter.encrypt(value)
        return value

    def decode(self, value):
        encrypter = self.task.encrypter
        if encrypter is not None:
            value = encrypter.decrypt(value)
        compressor = self.task.compressor
        if compressor is not None:
            value = compressor.decompress(value)
        serializer = self.task.serializer
        if serializer is not None:
            value = serializer.loads(value)
        for encoder in self.task.encoders:
            value = encoder.decode(value)
        return value

    def __call__(self, *args, **kwds):
        return self.task.func(*args, **kwds)
