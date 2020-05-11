from hypothesis import given
from hypothesis.strategies import integers, text, one_of

from shuttl_workflows.state import State
from shuttl_workflows.tasks import Task, TaskContext
import pytest


def some_func(x):
    return x * 2


@given(
    task_name=text(min_size=1, max_size=10),
)
def test_task_creation_with_name(task_name):

    task = Task(some_func, name=task_name)
    assert task.get_name() == task_name
    assert task.get_context().get_state() == State.CREATED


def test_task_creation_without_name():

    task = Task(some_func)
    assert task.get_name() == some_func.__name__
    assert task.get_context().get_state() == State.CREATED


@given(
    task_name=text(min_size=1, max_size=10),
    param=one_of([text(min_size=1), integers()]),
)
def test_task_execution_for_success_flow(task_name, param):

    expected_result = some_func(param)

    task = Task(some_func, name=task_name)
    assert task.get_context().get_state() == State.CREATED

    result = task(param)

    assert result == expected_result
    assert result == task.get_context().get_result()
    assert task.get_context().get_state() == State.SUCCESS


@given(
    task_name=text(min_size=1, max_size=10),
    param=one_of([text(min_size=1), integers()]),
)
def test_task_execution_for_error_flow(task_name, param):

    def func(x):
        raise Exception

    task = Task(func, name=task_name)
    assert task.get_context().get_state() == State.CREATED

    with pytest.raises(Exception):
        task(param)

    assert isinstance(task.get_context().get_error(), Exception)
    assert task.get_context().get_state() == State.FAILED


@given(
    task_name=text(min_size=1, max_size=10),
    param=one_of([text(min_size=1), integers()]),
)
def test_task_creation_with_context(task_name, param):

    def func(x, context: TaskContext = None):
        assert context.get_name() == task_name
        assert context.get_state() == State.RUNNING
        return x * 2

    expected_result = param * 2

    task = Task(func, name=task_name)
    assert task.get_context().get_state() == State.CREATED

    result = task(param)

    assert result == expected_result
    assert result == task.get_context().get_result()
    assert task.get_context().get_state() == State.SUCCESS


@given(
    task_name=text(min_size=1, max_size=10),
    param=one_of([text(min_size=1), integers()]),
)
def test_task_context_kv_store(task_name, param):

    key_1 = "key_1"
    value_1 = "value_1"

    key_2 = "key_2"
    value_2 = "value_2"

    def func(x, context: TaskContext = None):
        assert context.get_name() == task_name
        assert context.get_state() == State.RUNNING

        context.set(key_1, value_1)
        context.set(key_2, value_2)

        return x * 2

    expected_result = param * 2

    task = Task(func, name=task_name)
    assert task.get_context().get_state() == State.CREATED

    result = task(param)

    assert result == expected_result
    assert result == task.get_context().get_result()
    assert task.get_context().get_state() == State.SUCCESS

    assert task.get_context().get(key_1) == value_1
    assert task.get_context().get(key_2) == value_2
