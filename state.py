from exceptions import InvalidStateTransition


class State:
    def next(self, state: "State") -> "State":
        raise NotImplemented

    def raise_invalid_transition(self, next_state: "State"):
        raise InvalidStateTransition(f"Invalid transition from {self} to {next_state}")

    def __repr__(self):
        return type(self).__name__


class CREATED(State):
    def next(self, state: "State") -> "State":
        if state in [TaskState.QUEUED, TaskState.RUNNING]:
            return state
        self.raise_invalid_transition(state)


class QUEUED(State):
    def next(self, state: "State") -> "State":
        if state in [TaskState.RUNNING, TaskState.TERMINATED, TaskState.REJECTED]:
            return state
        self.raise_invalid_transition(state)


class RUNNING(State):
    def next(self, state: "State") -> "State":
        if state in [TaskState.FAILED, TaskState.SUCCESS]:
            return state
        self.raise_invalid_transition(state)


class TERMINATED(State):
    def next(self, state: "State") -> "State":
        if state in [TaskState.REJECTED]:
            return state
        self.raise_invalid_transition(state)


class REJECTED(State):
    def next(self, state: "State") -> "State":
        self.raise_invalid_transition(state)


class FAILED(State):
    def next(self, state: "State") -> "State":
        if state == TaskState.QUEUED:
            return state
        self.raise_invalid_transition(state)


class SUCCESS(State):
    def next(self, state: "State") -> "State":
        self.raise_invalid_transition(state)


class TaskState:
    """
                  +---------------------------+
                  |                           |
    *Created -> QUEUED -> RUNNING -> SUCCESS  |
                 | ^       |   |              |
                 | |       v   |              |
                 | +---Failed  v              |
                 |         |  TERMINATED <----+
                 |         |        |
                 |         v        |
                 +-----> Rejected <-+
    """
    CREATED = CREATED()
    QUEUED = QUEUED()
    RUNNING = RUNNING()
    TERMINATED = TERMINATED()
    REJECTED = REJECTED()
    FAILED = FAILED()
    SUCCESS = SUCCESS()

    def __init__(self):
        self._state = TaskState.CREATED

    def set_state(self, state: "State") -> None:
        self._state = self._state.next(state)

    def __eq__(self, other):
        return self._state == other

    def __repr__(self):
        return self._state.__repr__()
