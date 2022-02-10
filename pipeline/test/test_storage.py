from __future__ import annotations

from typing import Optional

import hypothesis.strategies as st
from hypothesis import given

from .. import Storage, task


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
            assert len(storage.fs) == 0
        else:
            assert len(storage.fs) == 1

        if SAVED is not None and task_save:
            # The result should be loaded
            assert result == SAVED
        else:
            assert result == counter.counter
