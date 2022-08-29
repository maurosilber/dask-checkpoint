from __future__ import annotations

from dask.base import tokenize as dask_tokenize


def function_name(func):
    return func.__name__


def tokenize(args, kwargs):
    return dask_tokenize(*args, **kwargs)
