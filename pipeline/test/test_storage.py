from __future__ import annotations

from typing import Optional

import hypothesis.strategies as st
import pytest
from hypothesis import given
from pytest import raises

from .. import DefaultEncoder, Storage, task


@given(inputs=st.lists(st.tuples(st.booleans(), st.booleans())))
def test_single_storage(inputs: list[tuple[bool, bool]]):
    """Tests that results are saved to/loaded from Storage, and/or the
    task is re-run for combinations of storage_save and task_save in:

    with storage(save=storage_save):
        my_task = task(..., save=task_save)
        my_task().compute()
    """

    # We create a Counter object with a non-pure function
    # to keep track of how many times it was executed, and
    # if its returning from Storage or computing the result.
    class Counter:
        def __init__(self):
            self.counter = 0

        def __call__(self):
            self.counter += 1
            return self.counter

    counter = Counter()
    storage = Storage({})

    RUNS: int = 0
    SAVED: Optional[int] = None
    for storage_save, task_save in inputs:
        # The function will be re-run if it hasn't been saved yet
        # or if the task is marked with save=False
        if SAVED is None or not task_save:
            RUNS += 1

        # The result will be saved when both the context manager and
        # the task are marked with save=True.
        # If it has already been saved, it won't be rewritten, but loaded.
        if SAVED is None and storage_save and task_save:
            # The saved value will correspond to the current task.compute(),
            # which should equal RUNS.
            SAVED = RUNS  # This must happen after increasing RUNS

        # Let's create and run the task.
        with storage(save=storage_save):
            counter_task = task(counter, name="counter", save=task_save)
            result = counter_task().compute()

        # The external counter should equal the number of runs
        assert counter.counter == RUNS

        if SAVED is None:
            assert len(storage.data) == 0
        else:
            assert len(storage.data) == 1

        if SAVED is not None and task_save:
            # The result should be loaded
            assert result == SAVED
        else:
            assert result == counter.counter


@pytest.mark.parametrize("save", [False, True])
def test_load_from_combined_storage(save: bool):
    """There are two ways of combining storages, which have different
    key search order:

    1. Chained storages:

    for key in keys:
        for storage in storages:
            if key in storage: ...

    2. Nested context managers:

    for storage in storages:
        for key in keys:
            if key in storage: ...

    We want to test that both ways produce the same result.
    """

    @task(save=True, encoder=DefaultEncoder(serializer=None, compressor=None))
    def uncomputable_task(x) -> bytes:
        """A task which cannot be computed, but it could be loaded."""
        raise RuntimeError

    # The task cannot be computed, but for inputs x=1 and x=2,
    # it is already saved in storage_1 and storage_2, respectively.
    task_1 = uncomputable_task(1)
    storage_1 = Storage({task_1.key: b"a"})

    task_2 = uncomputable_task(2)
    storage_2 = Storage({task_2.key: b"b"})

    # The full task will concatenate both strings.
    full_task = task_1 + task_2

    # We can't compute the full task,
    # as we can't compute any of its dependencies.
    with raises(RuntimeError):
        full_task.compute()

    # We can't compute it either with only one of the storages
    with raises(RuntimeError):
        with storage_1(save=save):
            full_task.compute()

    with raises(RuntimeError):
        with storage_2(save=save):
            full_task.compute()

    # We can "compute" it with both storages simultaneously.
    # Chained:
    chained_storage = Storage.from_chain(storage_1, storage_2)
    with chained_storage(save=save):
        assert full_task.compute() == b"ab"

    # Nested:
    with storage_1(save=save):
        with storage_2(save=save):
            assert full_task.compute() == b"ab"

    # Nested=False:
    with raises(RuntimeError):
        with storage_1(save=save):
            with storage_2(save=save, nested=False):
                assert full_task.compute() == b"ab"
