from unittest.mock import MagicMock

from hypothesis import given
from hypothesis.strategies import integers, text, one_of

from stream_processor.schedulers import ThreadPoolScheduler
from stream_processor.tasks import Task, State


@given(param=one_of([text(min_size=1), integers()]),)
def test_handlers_when_success_flow(param):
    def func(x):
        return x * 2

    queue_handler = MagicMock()
    start_handler = MagicMock()
    success_handler = MagicMock()

    scheduler = ThreadPoolScheduler()
    expected_result = func(param)

    task = Task(
        func,
        on_start_handlers=[start_handler],
        on_queue_handlers=[queue_handler],
        on_completion_success_handlers=[success_handler],
    )
    scheduler.add_task(task=task, params=param)

    assert queue_handler.called

    assert task.state == State.QUEUED

    assert not success_handler.called
    assert not start_handler.called

    result = scheduler.results()
    assert next(result) == expected_result

    assert start_handler.called

    assert success_handler.called
    handler_args = success_handler.call_args_list[0][0]
    assert handler_args[0] == expected_result
    assert task.state == State.SUCCESS


@given(param=one_of([text(min_size=1), integers()]),)
def test_handlers_when_error_flow(param):
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
        on_start_handlers=[start_handler],
        on_queue_handlers=[queue_handler],
        on_failure_handlers=[failure_handler],
    )
    scheduler.add_task(task=task, params=param)

    assert queue_handler.called

    assert task.state == State.QUEUED

    assert not failure_handler.called
    assert not start_handler.called

    result = scheduler.results()
    assert next(result) is None

    assert start_handler.called

    assert failure_handler.called
    handler_args = failure_handler.call_args_list[0][0]
    assert isinstance(handler_args[0], CustomException)
    assert task.state == State.FAILED
