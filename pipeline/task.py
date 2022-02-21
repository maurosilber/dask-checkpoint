from __future__ import annotations

from dataclasses import dataclass
from functools import cached_property
from inspect import BoundArguments, Parameter, signature
from types import MethodType, ModuleType
from typing import Generic, Optional, ParamSpec, TypeVar

import cloudpickle
import zstandard
from dask import delayed
from dask.base import tokenize
from dask.core import literal

from .encoder import Compressor, DefaultEncoder, Encoder, Encrypter, Serializer

T = TypeVar("T")
P = ParamSpec("P")


class dependency(Generic[T]):
    """Dependency non-data descriptor."""

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self.func

        return self.func(obj)


class Task(DefaultEncoder[T]):
    save: bool = False

    __bound: BoundArguments
    _delayed_run: callable

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

        By default: {Task name}/{hash from run parameters}.{suffixes from encoders}
        """
        name = self.__class__.__qualname__
        hash = self._hash()
        suffix = self._extension()
        return f"{name}/{hash}.{suffix}"

    def _hash(self) -> str:
        args, kwargs = self._run_params
        return tokenize(*args, **kwargs)

    def _extension(self) -> str:
        suffixes = []
        for cls in (self.serializer, self.compressor, self.encrypter):
            if cls is None:
                continue
            elif isinstance(cls, (type, ModuleType)):
                name = cls.__name__
            else:
                name = cls.__class__.__name__

            name = name.split(".")[-1]
            suffixes.append(name)
        return ".".join(suffixes)

    def __init_subclass__(cls):
        """Validates that a Task is well-specified."""
        super().__init_subclass__()

        # Convert to dataclass.
        dataclass(cls)
        cls.__signature__ = signature(MethodType(cls.__init__, cls))

        # Validate save
        if not isinstance(cls.save, bool):
            raise TypeError(f"{cls}.save must be a boolean: True or False.")

        # Validate run method:
        #   - convert to a staticmethod
        #   - check that all run parameters either exist as dependencies
        #     or are in __annotations__.
        run = cls.run
        cls.run = staticmethod(run)
        missing_parameters = {
            k
            for k in signature(run).parameters
            if k not in cls.__annotations__ and not hasattr(cls, k)
        }
        if len(missing_parameters) > 0:
            raise NameError(
                f"{cls}.run method has parameters with are neither an"
                f"annotation or a dependency: {missing_parameters}"
            )

        @delayed(pure=True, traverse=False)
        def _delayed_run(self, args, kwargs):
            # It needs to take a self parameter, since we need
            # the task instance to use its encode and decode methods.
            return run(*args, **kwargs)

        cls._delayed_run = _delayed_run

    def __new__(cls, *args, _delayed=True, **kwargs):
        # If _delayed=True, we return a task instance wrapped in dask.delayed.

        if cls is Task:
            raise NotImplementedError

        # Create instance
        self = super().__new__(cls)

        # Initialize instance attributes. Dependencies are overridden as they are
        # non-data descriptors.
        self.__bound = cls.__signature__.bind_partial(*args, **kwargs)
        for name, value in self.__bound.arguments.items():
            setattr(self, name, value)

        if _delayed:
            # Return the task instance as a dask.delayed function,
            # called with the task.run parameters.
            func = cls._delayed_run
            args, kwargs = self._run_params
            return func(literal(self), args, kwargs, dask_key_name=self.dask_key)
        else:
            # Return instance. Needed for Task serialization.
            return self

    def __getnewargs_ex__(self):
        # Enables task serialization. We need to build a Task instance,
        # not a dask.delayed function, hence the _delayed=False
        return self.__bound.args, {**self.__bound.kwargs, "_delayed": False}

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


def task(
    func: callable[P, T] = None,
    # /,  TODO: uncomment when we stop supporting Python < 3.8
    *,
    name: str = None,
    save: bool = False,
    encoders: Optional[tuple[Encoder]] = None,
    serializer: Optional[Serializer] = cloudpickle,
    compressor: Optional[Compressor] = zstandard,
    encrypter: Optional[Encrypter] = None,
) -> callable[P, T]:
    """Build a task from a callable. Can be used as a decorator."""
    if func is None:
        # We are being called as a decorator.
        kwargs = locals()
        kwargs.pop("func")

        def task_partial(func):
            return task(func, **kwargs)

        return task_partial

    # Get annotations and default parameters from function signature
    annotations, defaults = {}, {}
    for k, v in signature(func).parameters.items():
        annotations[k] = v.annotation
        if v.default is not Parameter.empty:
            defaults[k] = v.default

    namespace = {
        "run": func,
        "__annotations__": annotations,
        "save": save,
        "encoders": encoders,
        "serializer": serializer,
        "compressor": compressor,
        "encrypter": encrypter,
        **defaults,
    }
    return type(name or func.__name__, (Task,), namespace)
