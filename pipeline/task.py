from __future__ import annotations

from functools import cached_property
from inspect import Parameter, Signature, signature
from typing import Any, Mapping, TypeVar, get_type_hints

from dask import delayed
from dask.base import tokenize
from typing_extensions import Annotated

from .targets.core import Target


class MetaDependency(type):
    def __repr__(self):
        return "dependency"


class dependency(metaclass=MetaDependency):
    """Dependency descriptor."""

    def __init__(self, fget=None):
        self.fget = fget
        self.__doc__ = fget.__doc__

    def __get__(self, task, objtype=None):
        if task is None:
            return self.fget
        return self.fget(task)

    def __class_getitem__(cls, item):
        return DependencyType[item]

    def __set_name__(self, owner, name):
        """Set the return type in owner's annotations."""
        ret_type = get_type_hints(self.fget).get("return", Any)
        owner.__annotations__[name] = dependency[ret_type]


DependencyType = Annotated[TypeVar("T"), dependency]  # noqa: F821


class MetaTask(type):
    def __prepare__(name, bases, **kwargs) -> Mapping[str, Any]:
        return {"__annotations__": {}}

    def __init__(cls, name, bases, clsdict):
        check_run_signature(cls.run)
        cls.__signature__ = build_task_signature(cls)

        for method in ("target", "key"):
            method = getattr(cls, method)
            if not isinstance(method, property):
                raise Exception(f"Method {method.__qualname__} must be a property.")


def check_run_signature(method):
    """Run signature must not have keyword-only parameters."""
    parameters = signature(method).parameters
    kinds = (Parameter.KEYWORD_ONLY, Parameter.VAR_KEYWORD)
    if any(p.kind in kinds for p in parameters.values()):
        raise Exception("run parameters must be positional, not be keyword-only.")


def build_task_signature(instance):
    """Build task signature from type hints.

    Parameter order:
        - run parameters
        - class attributes
        - dependencies
    """
    parameters, kind = {}, Parameter.KEYWORD_ONLY
    for name, p in signature(instance.run).parameters.items():
        parameters[name] = Parameter(
            name, kind, default=p.default, annotation=p.annotation
        )

    for name, annotation in get_type_hints(instance).items():
        try:
            if dependency in annotation.__metadata__:
                default = Parameter.empty
        except AttributeError:
            default = getattr(instance, name, Parameter.empty)
        parameters[name] = Parameter(name, kind, default=default, annotation=annotation)
    return Signature(parameters.values())


class Task(metaclass=MetaTask):
    @staticmethod
    def run():
        """Compute the result.

        Run signature must have positional parameters, which are
        declared as class attributes or dependency methods.
        """
        raise NotImplementedError

    @property
    def target(self) -> Target:
        raise NotImplementedError

    def save(self, result):
        """Transform result before passing to target.save."""
        self.target.save(result)

    def load(self):
        """Transform result from target.load.

        Must be the inverse transformation of Task.save.
        """
        return self.target.load()

    @property
    def key(self) -> str:
        name = self.__class__.__qualname__
        return f"{name}-{tokenize(*self._run_args)}"

    def __new__(cls, ret_delayed=True, **kwargs):
        # Create instance and save kwargs for later reconstruction.
        self = super().__new__(cls)
        self.kwargs = kwargs

        # Initialize instance attributes.
        bound = cls.__signature__.bind_partial(**kwargs)
        for name, value in bound.arguments.items():
            setattr(self, name, value)

        if ret_delayed:
            func = delayed(self.run, name=cls.__qualname__)
            return func(*self._run_args, dask_key_name=self.key)
        else:
            return self

    def __getnewargs_ex__(self):
        return (), {**self.kwargs, "ret_delayed": False}

    @cached_property
    def _run_args(self):
        parameters = signature(self.run).parameters
        return tuple(getattr(self, k) for k in parameters)
