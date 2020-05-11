import inspect
from typing import Any, Optional, Callable

from shuttl_workflows.exceptions import TaskHandlerException, InvalidStateTransition
from shuttl_workflows.state import TaskState, State

OnQueueCallable = Callable[["TaskContext"], None]
OnStartCallable = Callable[["TaskContext"], None]
OnFailureCallable = Callable[[Exception, "TaskContext"], None]
OnRejectionCallable = Callable[["TaskContext"], None]
OnTerminationCallable = Callable[["TaskContext"], None]
OnCompletionSuccessCallable = Callable[[Any, "TaskContext"], None]


class TaskContext:
    def __init__(
        self,
        name: str,
        *,
        on_queue_handler: OnQueueCallable = None,
        on_start_handler: OnStartCallable = None,
        on_failure_handler: OnFailureCallable = None,
        on_rejection_handler: OnRejectionCallable = None,
        on_termination_handler: OnTerminationCallable = None,
        on_completion_success_handler: OnCompletionSuccessCallable = None,
        **kwargs,
    ):
        self._name = name
        self._task_state = TaskState()

        self._on_queue_handler = on_queue_handler or self._on_queue_handler
        self._on_start_handler = on_start_handler or self._on_start_handler
        self._on_failure_handler = on_failure_handler or self._on_failure_handler
        self._on_rejection_handler = on_rejection_handler or self._on_rejection_handler
        self._on_termination_handler = (
            on_termination_handler or self._on_termination_handler
        )
        self._on_completion_success_handler = (
            on_completion_success_handler or self._on_completion_success_handler
        )

        self._kv_store = {}
        self._result = None
        self._error = None

        for key, value in kwargs:
            self.set(key, value)

    def get_name(self) -> str:
        return self._name

    def get(self, key: str) -> Any:
        return self._kv_store.get(key)

    def set(self, key: str, value: Any) -> Any:
        self._kv_store[key] = value

    def get_result(self) -> Any:
        return self._result

    def set_result(self, result: Any) -> None:
        self._result = result

    def get_error(self) -> Exception:
        return self._error

    def set_error(self, error: Exception) -> None:
        self._error = error

    def set_state(self, state: "State") -> None:
        self._task_state.set_state(state)
        self._invoke_handlers_for_state_change()

    def get_state(self) -> TaskState:
        return self._task_state

    def _on_completion_success_handler(
        self, result: Any, context: "TaskContext"
    ) -> None:
        pass

    def _on_queue_handler(self, context: "TaskContext") -> None:
        pass

    def _on_start_handler(self, context: "TaskContext") -> None:
        pass

    def _on_rejection_handler(self, context: "TaskContext") -> None:
        pass

    def _on_termination_handler(self, context: "TaskContext") -> None:
        pass

    def _on_failure_handler(self, exception: Exception, context: "TaskContext") -> None:
        raise exception

    def _invoke_handlers_for_state_change(self) -> None:
        try:
            if self._task_state == State.QUEUED:
                self._on_queue_handler(self)

            if self._task_state == State.RUNNING:
                self._on_start_handler(self)

            if self._task_state == State.TERMINATED:
                self._on_termination_handler(self)

            if self._task_state == State.REJECTED:
                self._on_rejection_handler(self)

            if self._task_state == State.FAILED:
                self._on_failure_handler(self.get_error(), self)

            if self._task_state == State.SUCCESS:
                self._on_completion_success_handler(self._result, self)
        except Exception:
            raise TaskHandlerException


class Task:
    def __init__(
        self,
        func: Callable,
        *,
        name: str = None,
        on_queue_handler: OnQueueCallable = None,
        on_start_handler: OnStartCallable = None,
        on_failure_handler: OnFailureCallable = None,
        on_rejection_handler: OnRejectionCallable = None,
        on_termination_handler: OnTerminationCallable = None,
        on_completion_success_handler: OnCompletionSuccessCallable = None,
        **kwargs,
    ):
        self._func = func
        self._context = TaskContext(
            name or func.__name__,
            on_queue_handler=on_queue_handler,
            on_start_handler=on_start_handler,
            on_failure_handler=on_failure_handler,
            on_rejection_handler=on_rejection_handler,
            on_termination_handler=on_termination_handler,
            on_completion_success_handler=on_completion_success_handler,
            **kwargs,
        )

    def __call__(self, *args, **kwargs) -> Optional[Any]:
        self._context.set("args", args)
        self._context.set("kwargs", kwargs)
        return self._execute(*args, **kwargs)

    def get_name(self) -> str:
        return self._context.get_name()

    def get_context(self) -> TaskContext:
        return self._context

    def _execute(self, *args, **kwargs) -> Optional[Any]:
        result = None
        self._context.set_state(State.RUNNING)
        try:
            if "context" in inspect.getfullargspec(self._func).args:
                kwargs["context"] = self._context

            result = self._func(*args, **kwargs)
            self._context.set_result(result)
            self._context.set_state(State.SUCCESS)
        except TaskHandlerException:
            raise
        except InvalidStateTransition:
            raise
        except Exception as e:
            self._context.set_error(e)
            self._context.set_state(State.FAILED)

        return result
