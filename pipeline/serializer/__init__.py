from __future__ import annotations

from importlib import import_module


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

        name is

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
                module = import_module(self.module, "pipeline.serializer")
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
