from inspect import Parameter

from pytest import raises

from pipeline import Task, dependency


def test_no_parameters():
    class MyTask(Task):
        @staticmethod
        def run():
            return 42

    params = MyTask.__signature__.parameters
    assert len(params) == 0

    assert MyTask().compute() == 42

    with raises(TypeError):
        MyTask(1)

    with raises(TypeError):
        MyTask(a=1)


def test_run_positional_or_keyword():
    class MyTask(Task):
        @staticmethod
        def run(x):
            return x

    # Signature positional or keyword
    params = MyTask.__signature__.parameters
    assert len(params) == 1
    assert params["x"].kind == Parameter.POSITIONAL_OR_KEYWORD

    assert MyTask(1).compute() == 1
    assert MyTask(x=1).compute() == 1

    # No default
    with raises(AttributeError):
        assert MyTask()


def test_run_positional_only():
    class MyTask(Task):
        @staticmethod
        def run(x, /):
            return x

    # Signature positional only
    params = MyTask.__signature__.parameters
    assert len(params) == 1
    assert params["x"].kind == Parameter.POSITIONAL_ONLY

    assert MyTask(1).compute() == 1

    # Must be positional only
    with raises(TypeError):
        assert MyTask(x=1).compute() == 1

    # No default
    with raises(AttributeError):
        assert MyTask()


def test_run_and_class_default():
    class MyTask(Task):
        x: int = 42

        @staticmethod
        def run(x):
            return x

    # Signature is given by run as positional or keyword
    params = MyTask.__signature__.parameters
    assert len(params) == 1
    assert params["x"].kind == Parameter.POSITIONAL_OR_KEYWORD

    # Default
    assert MyTask().compute() == 42

    # Override default
    assert MyTask(1).compute() == 1
    assert MyTask(x=1).compute() == 1


def test_run_and_dependency():
    class MyTask(Task):
        @staticmethod
        def run(x):
            return x

        @dependency
        def x(self):
            return 42

    # Signature is given by run as positional or keyword
    params = MyTask.__signature__.parameters
    assert len(params) == 1
    assert params["x"].kind == Parameter.POSITIONAL_OR_KEYWORD

    # Default
    assert MyTask().compute() == 42

    # Override
    assert MyTask(1).compute() == 1
    assert MyTask(x=1).compute() == 1


def test_not_run_class_parameter():
    class MyTask(Task):
        y: int = 1

        @staticmethod
        def run(x):
            return x

        @dependency
        def x(self):
            return 2 * self.y

    # Signature is given by run as positional or keyword
    # while "y" does not appear as it is not a dependency
    params = MyTask.__signature__.parameters
    assert len(params) == 1
    assert params["x"].kind == Parameter.POSITIONAL_OR_KEYWORD

    # Default
    assert MyTask().compute() == 2

    # Override
    assert MyTask(1).compute() == 1
    assert MyTask(x=1).compute() == 1

    # Can't override class parameter
    with raises(TypeError):
        assert MyTask(y=1)


def test_not_run_dependency_parameter():
    class MyTask(Task):
        y: int = dependency(1)

        @staticmethod
        def run(x):
            return x

        @dependency
        def x(self):
            return 2 * self.y

    # Signature is given by run as positional or keyword
    # and "y" dependency as keyword only.
    params = MyTask.__signature__.parameters
    assert len(params) == 2
    assert params["x"].kind == Parameter.POSITIONAL_OR_KEYWORD
    assert params["y"].kind == Parameter.KEYWORD_ONLY

    # Default
    assert MyTask().compute() == 2

    # Override run parameter
    assert MyTask(1).compute() == 1
    assert MyTask(x=1).compute() == 1

    # Override dependency parameter
    assert MyTask(y=1).compute() == 2

    # Dependency is not positional
    with raises(TypeError):
        assert MyTask(1, 1)
