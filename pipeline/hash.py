from dask.base import tokenize


def default_hasher(*args, **kwargs):
    return tokenize(*args, **kwargs)
