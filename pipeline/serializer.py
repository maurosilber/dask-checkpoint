import abc
import io
import json

import blosc
import numpy as np


class Serializer(abc.ABC):
    @staticmethod
    @abc.abstractmethod
    def dumps(obj):
        """Serialize object."""

    @staticmethod
    @abc.abstractmethod
    def loads(obj):
        """Deserialize object."""


class NumpyNPY(Serializer):
    @staticmethod
    def dumps(x):
        with io.BytesIO() as buf:
            np.save(buf, x)
            return buf.getvalue()

    @staticmethod
    def loads(x):
        with io.BytesIO(x) as buf:
            return np.load(buf)


class JSON(Serializer):
    dumps = staticmethod(json.dumps)
    loads = staticmethod(json.loads)


class Blosc(Serializer):
    dumps = staticmethod(blosc.compress)
    loads = staticmethod(blosc.decompress)
