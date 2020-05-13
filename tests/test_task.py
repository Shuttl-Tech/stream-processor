from hypothesis import given
from hypothesis.strategies import integers, text, one_of

from stream_processor.tasks import Task, TaskContext, State


def some_func(x):
    return x * 2


@given(param=one_of([text(min_size=1), integers()]),)
def test_task_execution_for_success_flow(param):

    expected_result = some_func(param)

    task = Task(some_func)
    assert task.state == State.CREATED

    result = task(param)

    assert result == expected_result
    assert result == task.result
    assert task.state == State.SUCCESS


@given(param=one_of([text(min_size=1), integers()]),)
def test_task_execution_for_error_flow(param):
    def func(x):
        raise Exception

    task = Task(func)
    assert task.state == State.CREATED

    result = task(param)

    assert result is None
    assert isinstance(task.error, Exception)
    assert task.state == State.FAILED


@given(param=one_of([text(min_size=1), integers()]),)
def test_task_creation_with_context(param):
    def func(x, context: TaskContext = None):
        assert context.state == State.RUNNING
        return x * 2

    expected_result = param * 2

    task = Task(func)
    assert task.state == State.CREATED

    result = task(param)

    assert result == expected_result
    assert result == task.result
    assert task.state == State.SUCCESS


@given(param=one_of([text(min_size=1), integers()]),)
def test_task_context_kv_store(param):

    key_1 = "key_1"
    value_1 = "value_1"

    key_2 = "key_2"
    value_2 = "value_2"

    def func(x, context: TaskContext = None):
        assert context.state == State.RUNNING

        context[key_1] = value_1
        context[key_2] = value_2

        return x * 2

    expected_result = param * 2

    task = Task(func)
    assert task.state == State.CREATED

    result = task(param)

    assert result == expected_result
    assert result == task.result
    assert task.state == State.SUCCESS

    assert task.context[key_1] == value_1
    assert task.context[key_2] == value_2
