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
