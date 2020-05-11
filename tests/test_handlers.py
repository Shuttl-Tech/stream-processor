from unittest.mock import MagicMock

from hypothesis import given
from hypothesis.strategies import integers, text, one_of

from shuttl_workflows.schedulers import ThreadPoolScheduler
from shuttl_workflows.state import State
from shuttl_workflows.tasks import Task


@given(
    task_name=text(min_size=1, max_size=10),
    param=one_of([text(min_size=1), integers()]),
)
def test_handlers_when_success_flow(task_name, param):

    def func(x):
        return x*2

    queue_handler = MagicMock()
    start_handler = MagicMock()
    success_handler = MagicMock()

    scheduler = ThreadPoolScheduler()
    expected_result = func(param)

    task = Task(
        func,
        name=task_name,
        on_start_handler=start_handler,
        on_queue_handler=queue_handler,
        on_completion_success_handler=success_handler
    )
    scheduler.add_task(task=task, params=param)

    assert queue_handler.called
    context = queue_handler.call_args_list[0][0]

    assert context[0].get_name() == task_name
    assert context[0].get_state() == State.QUEUED

    assert not success_handler.called
    assert not start_handler.called

    result = scheduler.results()
    assert next(result) == expected_result

    assert start_handler.called
    context = start_handler.call_args_list[0][0]
    assert context[0].get_name() == task_name

    assert success_handler.called
    results, context = success_handler.call_args_list[0][0]
    assert results == expected_result
    assert context.get_name() == task_name
    assert context.get_state() == State.SUCCESS


@given(
    task_name=text(min_size=1, max_size=10),
    param=one_of([text(min_size=1), integers()]),
)
def test_handlers_when_error_flow(task_name, param):

    class CustomException(Exception):
        pass

    def func(x):
        raise CustomException

    queue_handler = MagicMock()
    start_handler = MagicMock()
    failure_handler = MagicMock()

    scheduler = ThreadPoolScheduler()

    task = Task(
        func,
        name=task_name,
        on_start_handler=start_handler,
        on_queue_handler=queue_handler,
        on_failure_handler=failure_handler
    )
    scheduler.add_task(task=task, params=param)

    assert queue_handler.called
    context = queue_handler.call_args_list[0][0]

    assert context[0].get_name() == task_name
    assert context[0].get_state() == State.QUEUED

    assert not failure_handler.called
    assert not start_handler.called

    result = scheduler.results()
    assert next(result) is None

    assert start_handler.called
    context = start_handler.call_args_list[0][0]
    assert context[0].get_name() == task_name

    assert failure_handler.called
    error, context = failure_handler.call_args_list[0][0]
    assert isinstance(error, CustomException)
    assert context.get_name() == task_name
    assert context.get_state() == State.FAILED
