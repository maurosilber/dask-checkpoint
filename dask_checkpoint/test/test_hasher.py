from pytest import raises

from .. import task
from ..hasher import exclude


def test_positional_or_keyword():
    @task
    def func(x):
        pass

    assert func(1).key == func(x=1).key


def test_exclude_arguments():
    class Unhashable:
        def __dask_tokenize__(self):
            raise RuntimeError

    @task(hasher=exclude("y"))
    def func(x, y):
        pass

    u = Unhashable()

    funcs = [func(1, u), func(1, y=u), func(x=1, y=u)]
    assert len(set(f.key for f in funcs)) == 1

    with raises(RuntimeError):
        func(u, 1)

    with raises(RuntimeError):
        func(u, y=1)

    with raises(RuntimeError):
        func(x=u, y=1)
