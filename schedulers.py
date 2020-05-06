import itertools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator, Tuple

DEFAULT_MAX_WORKERS = 4


class SerialScheduler:
    def __init__(self):
        self._tasks = iter(())

    def add_task(self, task: Callable, param) -> None:
        self._tasks = itertools.chain(self._tasks, (task, param))

    def add_tasks(self, tasks: Tuple[Iterator, Iterator]) -> None:
        self._tasks = itertools.chain(self._tasks, tasks)

    def results(self) -> Iterator:
        return (task(params) for task, params in self._tasks)


class ThreadPoolScheduler:
    def __init__(self, max_workers=None):
        self._max_workers = max_workers or DEFAULT_MAX_WORKERS
        self._tasks = iter(())

    def add_task(self, task: Callable, param) -> None:
        self._tasks = itertools.chain(self._tasks, (task, param))

    def add_tasks(self, tasks: Tuple[Iterator, Iterator]) -> None:
        self._tasks = itertools.chain(self._tasks, tasks)

    def results(self) -> Iterator:
        pool = ThreadPoolExecutor(max_workers=self._max_workers)
        task_futures = (pool.submit(task, params) for task, params in self._tasks)
        return (r.result() for r in task_futures)

