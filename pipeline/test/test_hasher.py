from pipeline import task


def test_positional_or_keyword():
    @task
    def func(x):
        pass

    assert func(1).key == func(x=1).key
