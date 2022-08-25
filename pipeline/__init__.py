import dask

from .serializer import serializer
from .storage import Storage
from .task import task

dask.config.set({"delayed_pure": True, "tokenize.ensure-deterministic": True})

__all__ = ["Storage", "serializer", "task"]
