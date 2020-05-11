import itertools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator, Any, Union

from shuttl_workflows.tasks import Task, State

DEFAULT_MAX_WORKERS = 5


class Scheduler:
    def __init__(self):
        self._tasks: Iterator[Task] = iter(())

    def terminate_tasks(self) -> None:
        for task in self._tasks:
            task.get_context().set_state(State.TERMINATED)

    def reject_tasks(self) -> None:
        for task in self._tasks:
            task.get_context().set_state(State.REJECTED)

    def add_task(
        self, task: Union[Task, Callable], params: Any = None, name: str = None
    ) -> None:
        if not isinstance(task, Task):
            task = Task(task, name=name)
        self._tasks = itertools.chain(self._tasks, ((task, params),))
        task.get_context().set_state(State.QUEUED)

    def results(self) -> Iterator:
        pass


class SerialScheduler(Scheduler):
    def __init__(self):
        super().__init__()

    def results(self) -> Iterator:
        return (task(params) for task, params in self._tasks)


class ThreadPoolScheduler(Scheduler):
    def __init__(self, max_workers=None):
        super().__init__()
        self._max_workers = max_workers or DEFAULT_MAX_WORKERS
        self._pool = ThreadPoolExecutor(max_workers=self._max_workers)

    def results(self) -> Iterator:
        task_futures = [self._pool.submit(task, params) for task, params in self._tasks]
        return (r.result() for r in task_futures)


class SchedulerFactory:
    def __new__(cls, scheduler_name, *args, **kwargs) -> "Scheduler":
        if scheduler_name == "ThreadPool":
            return ThreadPoolScheduler(*args, **kwargs)
        if scheduler_name == "Serial":
            return SerialScheduler()
