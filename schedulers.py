import itertools
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterator, Tuple, Any, Union
from tasks import Task, TaskContext, TaskState
from abc import ABC
from collections import defaultdict

DEFAULT_MAX_WORKERS = 4


class Scheduler(ABC):
    def add_task(self, task: Callable, param: Any) -> None:
        pass

    def add_tasks(self, tasks: Tuple[Iterator, Iterator]) -> None:
        pass

    def results(self) -> Iterator:
        pass


class SerialScheduler(Scheduler):
    def __init__(self):
        self._tasks = iter(())

    def add_task(self, task: Union[Task, Callable], param: Any) -> None:
        self._tasks = itertools.chain(self._tasks, (task, param))

    def results(self) -> Iterator:
        return (task(params) for task, params in self._tasks)


class ThreadPoolScheduler(Scheduler):
    def __init__(self, max_workers=None):
        self._on_complete_handlers = defaultdict(list)
        self._max_workers = max_workers or DEFAULT_MAX_WORKERS
        self._tasks = iter(())

    def register_on_complete_handler(self, name: str, handler: Callable) -> None:
        self._on_complete_handlers[name].append(handler)

    def _on_task_complete(self, result: Any, context: TaskContext) -> None:
        name = context.get_name()
        for handler in self._on_complete_handlers[name]:
            handler(result, context)

    def add_task(
        self, task: Union[Task, Callable], params: Any = None, name: str = None
    ) -> None:
        if not isinstance(task, Task):
            task = Task(task, name=name)
        self.register_on_complete_handler(
            name=name, handler=task.get_context().on_complete
        )  # Making Scheduler's on_task_complete as default handler
        task.set_on_complete_handler(self._on_task_complete)
        self._tasks = itertools.chain(self._tasks, (task, params))
        task.get_context().set_state(TaskState.QUEUED)

    def results(self) -> Iterator:
        pool = ThreadPoolExecutor(max_workers=self._max_workers)
        task_futures = [pool.submit(task, params) for task, params in self._tasks]
        return (r.result() for r in task_futures)
