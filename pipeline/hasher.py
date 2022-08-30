from __future__ import annotations

from collections.abc import Callable

try:
    from typing import TypeAlias
except ImportError:
    from typing_extensions import TypeAlias

from dask.base import tokenize as dask_tokenize

FunctionHasher: TypeAlias = Callable[[Callable], str]
ArgumentHasher: TypeAlias = Callable[[dict], str]


def function_name(func):
    """Function name with a trailing slash."""
    return f"{func.__name__}/"


def tokenize(kwargs):
    return dask_tokenize(**kwargs)
