from __future__ import annotations

from io import BytesIO

import numpy as np


class npy:
    @staticmethod
    def dumps(x: np.ndarray) -> bytes:
        if not isinstance(x, np.ndarray):
            raise TypeError("Input must be a numpy.ndarray")

        file = BytesIO()
        np.save(file, x)
        return file.getvalue()

    @staticmethod
    def loads(x: bytes) -> np.ndarray:
        return np.load(BytesIO(x))


class npz:
    @staticmethod
    def dumps(x: dict[str, np.ndarray]) -> bytes:
        if not isinstance(x, dict):
            raise TypeError("Input must be a dict[str, numpy.ndarray].")

        if any(type(xi) is not np.ndarray for xi in x.values()):
            raise TypeError("Inputs must be instances of numpy.ndarray")

        f = BytesIO()
        np.savez(f, **x)
        return f.getvalue()

    @staticmethod
    def loads(x) -> dict[str, np.ndarray]:
        f = BytesIO(x)
        npz = np.load(f)
        return dict(npz.items())
