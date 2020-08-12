from pathlib import Path

import dask
from dask.base import normalize_token

dask.config.set(delayed_pure=True)


@normalize_token.register(Path)
def path_to_absolute(path):
    return str(path.resolve())
