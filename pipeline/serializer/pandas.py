from __future__ import annotations

from functools import partial
from io import BytesIO

import pandas as pd


class pandas:
    dump: callable
    load: callable

    @classmethod
    def dumps(cls, x: pd.DataFrame) -> bytes:
        if not isinstance(x, pd.DataFrame):
            raise TypeError("Input must be a pandas.DataFrame")

        file = BytesIO()
        cls.dump(x, file)
        return file.getvalue()

    @classmethod
    def loads(cls, x: bytes) -> pd.DataFrame:
        return cls.load(BytesIO(x))


class feather(pandas):
    dump = partial(pd.DataFrame.to_feather, compression=None)
    load = pd.read_feather


class parquet(pandas):
    dump = partial(pd.DataFrame.to_parquet, compression=None)
    load = pd.read_parquet
