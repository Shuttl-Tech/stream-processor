import inspect
from enum import Enum
from typing import Any, Optional, Callable, Dict


class TaskState(Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    ERROR = "ERROR"
    SUCCESS = "SUCCESS"


class TaskContext:
    def __init__(
        self,
        name: str,
        *,
        state: TaskState = None,
        on_complete: Callable = None,
        **kwargs
    ):
        self._state = state
        self._name = name
        self._kv_store = {}
        self._results = None
        if on_complete:
            self.on_complete = on_complete
        for key, value in kwargs:
            self.set(key, value)

    def get_name(self) -> str:
        return self._name

    def on_complete(self, result: str = None, context: str = None) -> None:
        pass

    def get_results(self) -> Any:
        return self._results

    def set_results(self, results: Any) -> None:
        self._results = results

    def set_state(self, state: TaskState) -> None:
        self._state = state

    def get_state(self) -> TaskState:
        return self._state

    def get(self, key: str) -> Any:
        return self._kv_store.get(key)

    def set(self, key: str, value: Any) -> Any:
        self._kv_store[key] = value


class Task:
    def __init__(
        self,
        func: Callable,
        *,
        name: str = None,
        on_complete: Callable = None,
        **kwargs
    ):
        self._func = func
        self._context = TaskContext(
            name or func.__name__, on_complete=on_complete, **kwargs
        )
        self._context.set_state(TaskState.CREATED)

    def __call__(self, *args, **kwargs) -> Optional[Any]:
        self._context.set("args", args)
        self._context.set("kwargs", kwargs)
        return self._execute(*args, **kwargs)

    def get_name(self) -> str:
        return self._context.get_name()

    def get_context(self) -> TaskContext:
        return self._context

    def set_on_complete_handler(self, handler: Callable) -> None:
        self._context._on_complete = handler

    def _execute(self, *args, **kwargs) -> Optional[Any]:
        result = None
        self._context.set_state(TaskState.RUNNING)
        try:
            if "context" in inspect.getfullargspec(self._func).args:
                kwargs["context"] = self._context

            result = self._func(*args, **kwargs)
            self._context.set_state(TaskState.SUCCESS)
            self._context.set_results(result)
            self._context.on_complete(
                result=result, context=self._context
            )
        except Exception as e:
            self._context.set_state(TaskState.ERROR)
            self._context.set_results(e)
            self._context.on_complete(
                result=e, context=self._context
            )
        return result
