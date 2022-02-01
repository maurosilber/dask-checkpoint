from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from inspect import BoundArguments, Parameter, signature
from typing import Generic, Protocol, TypeVar, runtime_checkable

from dask import delayed
from dask.base import tokenize

T = TypeVar("T")


@runtime_checkable
class Serializer(Protocol[T]):
    def dumps(x: T) -> bytes:
        ...

    def loads(x: bytes) -> T:
        ...


@runtime_checkable
class Compressor(Protocol):
    def compress(x: bytes) -> bytes:
        ...

    def decompress(x: bytes) -> bytes:
        ...


@runtime_checkable
class Encrypter(Protocol):
    def encrypt(x: bytes) -> bytes:
        ...

    def decrypt(x: bytes) -> bytes:
        ...


class dependency(Generic[T]):
    """Dependency non-data descriptor."""

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self.func

        return self.func(obj)


class Task(Generic[T]):
    __bound: BoundArguments

    @staticmethod
    def run() -> T:
        """Compute the result.

        Parameter names in run signature must be at least one of:
            - class annotation
            - class attribute
            - @dependency-decorated method
        """
        raise NotImplementedError

    @cached_property
    def dask_key(self) -> str:
        """Unique name for a given Task.

        It is the name used for the dask graph and to store the result.

        By default: {Task name}/{hash from run parameters}
        """
        name = self.__class__.__qualname__

        args, kwargs = self._run_params
        hash = tokenize(*args, **kwargs)

        return f"{name}/{hash}"

    def __init_subclass__(cls):
        """Validates that a Task is well-specified."""

        # Convert to dataclass.
        dataclass(cls)

        # Validate run method:
        #   - convert to a staticmethod
        #   - check that all run parameters either exist as dependencies
        #     or are in __annotations__.
        cls.run = staticmethod(cls.run)
        missing_parameters = {
            k
            for k in signature(cls.run).parameters
            if k not in cls.__annotations__ and not hasattr(cls, k)
        }
        if len(missing_parameters) > 0:
            raise NameError(
                f"{cls}.run method has parameters with are neither an"
                f"annotation or a dependecy: {missing_parameters}"
            )

    def __new__(cls, *args, _delayed=True, **kwargs):
        # If _delayed=True, we return a task instance wrapped in dask.delayed.

        if cls is Task:
            raise NotImplementedError

        # Create instance
        self = super().__new__(cls)

        # Initialize instance attributes. Dependencies are overriden as they are
        # non-data descriptors.
        self.__bound = signature(self.__init__).bind_partial(*args, **kwargs)
        for name, value in self.__bound.arguments.items():
            setattr(self, name, value)

        if _delayed:
            # Return the task instance as a dask.delayed function,
            # called with the task.run parameters.
            func = delayed(self, pure=True, traverse=False)
            args, kwargs = self._run_params
            return func(*args, **kwargs, dask_key_name=self.dask_key)
        else:
            # Return instance. Needed for Task serialization.
            return self

    def __getnewargs_ex__(self):
        # Enables task serialization. We need to build a Task instance,
        # not a dask.delayed function, hence the _delayed=False
        return self.__bound.args, {**self.__bound.kwargs, "_delayed": False}

    def __call__(self, *args, **kwargs):
        # Dask considers callables as tasks in its graph.
        # Hence, we can pass the task itself as the callable,
        # and later have access to dask_key, encode and decode.
        return self.run(*args, **kwargs)

    @cached_property
    def _run_params(self) -> tuple[tuple, dict]:
        # Get run function parameters from its signature, and
        # collect them from instance or class attributes/dependencies,
        args, kwargs = [], {}
        for name, param in signature(self.run).parameters.items():
            value = getattr(self, name)

            if param.kind <= Parameter.POSITIONAL_OR_KEYWORD:
                args.append(value)
            elif param.kind == Parameter.VAR_POSITIONAL:
                args.extend(value)  # value is an iterable
            elif param.kind == Parameter.KEYWORD_ONLY:
                kwargs[name] = value
            else:
                kwargs.update(value)  # value is a mapping

        return args, kwargs
