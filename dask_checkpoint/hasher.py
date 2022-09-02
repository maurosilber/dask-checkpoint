from __future__ import annotations

from collections.abc import Callable
from types import FunctionType

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


def exclude(*names: str, hasher: ArgumentHasher = tokenize):
    def exclude_hasher(kwargs):
        for name in names:
            del kwargs[name]
        return hasher(kwargs)

    return exclude_hasher


def function_hash(func: FunctionType):
    """Function name and a hash of its bytecode,
    with a trailing slash.

    >>> def func(x):
    ...     return 2 * x
    >>> function_hash(func)
    'func/1809a749c365cec5bc81ad24a37a794d/'
    """
    name = func.__name__
    code = func.__code__.co_code
    code_hash = dask_tokenize(code)
    return f"{name}/{code_hash}/"
