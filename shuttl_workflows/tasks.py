import inspect
from enum import Enum
from typing import Any, Optional, Callable, List

from shuttl_workflows.exceptions import TaskHandlerException, InvalidStateTransition

OnQueueCallable = Callable[[Optional["TaskContext"]], None]
OnStartCallable = Callable[[Optional["TaskContext"]], None]
OnFailureCallable = Callable[[Exception, Optional["TaskContext"]], None]
OnRejectionCallable = Callable[[Optional["TaskContext"]], None]
OnTerminationCallable = Callable[[Optional["TaskContext"]], None]
OnCompletionSuccessCallable = Callable[[Any, Optional["TaskContext"]], None]


class State(Enum):
    """
                      +---------------------------+
                      |                           |
              +-------+--------+                  |
              |       |        v                  |
        *Created -> QUEUED -> RUNNING -> SUCCESS  |
                     |  ^      |   |              |
                     |  |      v   |              |
                     |  +-- Failed v              |
                     |         |  TERMINATED <----+
                     |         |        |
                     |         v        |
                     +-----> Rejected <-+
        """

    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"
    SUCCESS = "SUCCESS"


MOVES = {
    State.CREATED: [State.QUEUED, State.RUNNING],
    State.QUEUED: [State.RUNNING, State.REJECTED, State.TERMINATED],
    State.RUNNING: [State.FAILED, State.TERMINATED, State.SUCCESS],
    State.FAILED: [State.REJECTED, State.QUEUED],
    State.TERMINATED: [State.REJECTED],
    State.SUCCESS: [],
    State.REJECTED: [],
}


class TaskContext:
    def __init__(
        self,
        *,
        on_queue_handlers: List[OnQueueCallable] = None,
        on_start_handlers: List[OnStartCallable] = None,
        on_failure_handlers: List[OnFailureCallable] = None,
        on_rejection_handlers: List[OnRejectionCallable] = None,
        on_termination_handlers: List[OnTerminationCallable] = None,
        on_completion_success_handlers: List[OnCompletionSuccessCallable] = None,
        **kwargs,
    ):
        self._state = State.CREATED

        self._on_queue_handlers = on_queue_handlers or []
        self._on_start_handlers = on_start_handlers or []
        self._on_failure_handlers = on_failure_handlers or []
        self._on_rejection_handlers = on_rejection_handlers or []
        self._on_termination_handlers = on_termination_handlers or []
        self._on_completion_success_handlers = on_completion_success_handlers or []

        self._kv_store = {}
        self._result = None
        self._error = None

        for key, value in kwargs.items():
            self[key] = value

    @property
    def result(self) -> Any:
        return self._result

    @result.setter
    def result(self, result: Any) -> None:
        self._result = result

    @property
    def error(self) -> Exception:
        return self._error

    @error.setter
    def error(self, error: Exception) -> None:
        self._error = error

    @property
    def state(self) -> State:
        return self._state

    @state.setter
    def state(self, state: State) -> None:
        if not self._is_valid_move(self._state, state):
            raise InvalidStateTransition(f"Invalid transition from {self.state} to {state}")
        self._state = state
        self._invoke_handlers_for_state_change()

    @staticmethod
    def _is_valid_move(from_state, to_state):
        return to_state in MOVES.get(from_state, [])

    def _publish_event(self, handlers: List[Callable], *args, **kwargs) -> None:
        for handler in handlers:
            if "context" in inspect.getfullargspec(handler).args:
                kwargs["context"] = self
            handler(*args, **kwargs)

    def _invoke_handlers_for_state_change(self) -> None:
        try:
            if self.state == State.QUEUED:
                self._publish_event(self._on_queue_handlers)

            if self.state == State.RUNNING:
                self._publish_event(self._on_start_handlers)

            if self.state == State.TERMINATED:
                self._publish_event(self._on_termination_handlers)

            if self.state == State.REJECTED:
                self._publish_event(self._on_rejection_handlers)

            if self.state == State.FAILED:
                self._publish_event(self._on_failure_handlers, self.error)

            if self.state == State.SUCCESS:
                self._publish_event(self._on_completion_success_handlers, self.result)

        except Exception:
            raise TaskHandlerException

    def get(self, key: str) -> Any:
        return self._kv_store.get(key)

    def __getitem__(self, key: str) -> Any:
        return self._kv_store[key]

    def __setitem__(self, key: str, value: Any) -> Any:
        self._kv_store[key] = value


class Task:
    def __init__(
        self,
        func: Callable,
        *,
        on_queue_handlers: List[OnQueueCallable] = None,
        on_start_handlers: List[OnStartCallable] = None,
        on_failure_handlers: List[OnFailureCallable] = None,
        on_rejection_handlers: List[OnRejectionCallable] = None,
        on_termination_handlers: List[OnTerminationCallable] = None,
        on_completion_success_handlers: List[OnCompletionSuccessCallable] = None,
        **kwargs,
    ):
        self._func = func
        self.context = TaskContext(
            on_queue_handlers=on_queue_handlers,
            on_start_handlers=on_start_handlers,
            on_failure_handlers=on_failure_handlers,
            on_rejection_handlers=on_rejection_handlers,
            on_termination_handlers=on_termination_handlers,
            on_completion_success_handlers=on_completion_success_handlers,
            **kwargs,
        )

    def __call__(self, *args, **kwargs) -> Optional[Any]:
        self.context["args"] = args
        self.context["kwargs"] = kwargs
        return self._execute(*args, **kwargs)

    @property
    def state(self) -> State:
        return self.context.state

    @state.setter
    def state(self, state: State) -> None:
        self.context.state = state

    @property
    def result(self) -> Any:
        return self.context.result

    @property
    def error(self) -> Exception:
        return self.context.error

    def _execute(self, *args, **kwargs) -> Optional[Any]:
        result = None
        self.context.state = State.RUNNING
        try:
            if "context" in inspect.getfullargspec(self._func).args:
                kwargs["context"] = self.context

            result = self._func(*args, **kwargs)
            self.context.result = result
            self.context.state = State.SUCCESS
        except TaskHandlerException:
            raise
        except InvalidStateTransition:
            raise
        except Exception as e:
            self.context.error = e
            self.context.state = State.FAILED

        return result
