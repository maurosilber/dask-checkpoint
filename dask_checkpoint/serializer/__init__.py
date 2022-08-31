from __future__ import annotations

from importlib import import_module

from ..encoder import Serializer


class LazyLoader:
    name: str
    module: str
    extra_requires: list[str]

    def __init__(self, module: str = None, *, extra_requires: str | list[str] = None):
        """Delayed loading of module or attribute of module.

        There are 3 variants:
            - from module import name
            - from .module import name  (if module starts with ".")
            - import name  (if module is None)

        Parameters
        ----------
        module : str, optional
            Module name to load.
        extra_requires : str, optional
            Dependencies needed to load module, by default None.

        Attributes
        ----------
        name : str
            Auto-assigned by __set_name__.
        """
        self.module = module

        if extra_requires is None:
            extra_requires = []
        elif isinstance(extra_requires, str):
            extra_requires = [extra_requires]
        self.extra_requires = extra_requires

    def __set_name__(self, cls, name):
        self.name = name

    def __get__(self, obj, cls):
        try:
            if self.module is None:
                return import_module(self.name)
            else:
                module = import_module(self.module, "dask_checkpoint.serializer")
                return getattr(module, self.name)
        except (AttributeError, ModuleNotFoundError):
            requirements = [self.module or self.name, *self.extra_requires]
            raise ModuleNotFoundError(
                f"To use the {self.name} serializer, you must install: {requirements}"
            )


class meta(type):
    def __repr__(self):
        serializers = [k for k, v in self.__dict__.items() if isinstance(v, LazyLoader)]
        serializers = ", ".join(sorted(serializers))
        return f"Available serializers: {serializers}"


class serializer(metaclass=meta):
    cloudpickle = LazyLoader()

    # Requires numpy
    npy = LazyLoader(".numpy", extra_requires="numpy")
    npz = LazyLoader(".numpy", extra_requires="numpy")

    # Requires pandas
    feather = LazyLoader(".pandas", extra_requires="pyarrow")
    parquet = LazyLoader(".pandas", extra_requires="pyarrow")

    # Requires to install serializer github.com/hgrecco/serializer
    bson = LazyLoader("serialize", extra_requires="bson")
    dill = LazyLoader("serialize", extra_requires="dill")
    json = LazyLoader("serialize", extra_requires="simplejson")
    msgpack = LazyLoader("serialize", extra_requires="msgpack-python")
    phpserialize = LazyLoader("serialize", extra_requires="phpserialize")
    serpent = LazyLoader("serialize", extra_requires="serpent")
    yaml = LazyLoader("serialize", extra_requires="pyyaml")


def __getattr__(name: str) -> Serializer:
    """To support imports from serializer class:

    from dask_checkpoint.serializer import <name>
    """
    return getattr(serializer, name)
