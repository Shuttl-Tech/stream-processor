from shuttl_workflows.exceptions import InvalidStateTransition
from enum import Enum


class State(Enum):
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


def is_valid_move(from_state, to_state):
    return to_state in MOVES.get(from_state, [])


class TaskState:
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

    def __init__(self):
        self._state = State.CREATED

    def set_state(self, state: "State") -> None:
        if not is_valid_move(self._state, state):
            raise InvalidStateTransition(f"Invalid transition from {self} to {state}")
        self._state = state

    def __eq__(self, other):
        return self._state == other

    def __repr__(self):
        return self._state.name
