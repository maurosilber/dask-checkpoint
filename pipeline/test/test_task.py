from pytest import raises

from .. import task


def test_deterministic_token():
    @task
    def func(x):
        return x

    assert func(0).key == func(0).key

    with raises(RuntimeError):
        # Cannot produce a determnistic token.
        func(object())


def test_no_argument():
    @task
    def func():
        return 42

    assert func().compute() == 42


def test_single_argument():
    @task
    def func(x):
        return 2 * x

    assert func(1).compute() == 2
    assert func(x=1).compute() == 2


def test_multiple_arguments():
    @task
    def func(x, y):
        return x + y

    assert func(2, 3).compute() == (2 + 3)
    assert func(2, y=3).compute() == (2 + 3)
    assert func(x=2, y=3).compute() == (2 + 3)


def test_dependencies():
    @task
    def func(x):
        return 2 * x

    x = 1
    y = func(x=x)
    z = func(x=y).compute()
    assert z == 4
