import itertools
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator, Any, Union, Type

from stream_processor.exceptions import InvalidTask
from stream_processor.tasks import Task, State

DEFAULT_MAX_WORKERS = 5


class Scheduler(ABC):
    def __init__(self):
        self.tasks: Iterator[Task] = iter(())

    def terminate_tasks(self) -> None:
        for task in self.tasks:
            task.state = State.TERMINATED

    def reject_tasks(self) -> None:
        for task in self.tasks:
            task.state = State.REJECTED

    def add_task(self, task: Union[Task, Callable], params: Any = None) -> None:

        if isinstance(task, Task):
            task = task
        elif isinstance(task, Callable):
            task = Task(task)
        else:
            raise InvalidTask("Expected Callable or instance of Task")

        self.tasks = itertools.chain(self.tasks, ((task, params),))
        task.state = State.QUEUED

    @abstractmethod
    def results(self) -> Iterator:
        raise NotImplemented


class SerialScheduler(Scheduler):
    def results(self) -> Iterator:
        return (task(params) for task, params in self.tasks)


class ThreadPoolScheduler(Scheduler):
    def __init__(self, max_workers=None):
        super().__init__()
        self._max_workers = max_workers or DEFAULT_MAX_WORKERS
        self._pool = ThreadPoolExecutor(max_workers=self._max_workers)

    def results(self) -> Iterator:
        task_futures = [self._pool.submit(task, params) for task, params in self.tasks]
        return (r.result() for r in task_futures)


class SchedulerFactory:
    def __new__(cls, classname: Type["Scheduler"], *args, **kwargs) -> "Scheduler":
        return classname(*args, **kwargs)
