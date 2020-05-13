from hypothesis import given
from hypothesis.strategies import integers, text, one_of

from stream_processor.schedulers import ThreadPoolScheduler, SerialScheduler
from stream_processor.tasks import Task, State


def some_func(x):
    return x * 2


@given(param=one_of([text(min_size=1), integers()]),)
def test_scheduler_add_task_accepts_callable(param):
    scheduler = SerialScheduler()
    scheduler.add_task(some_func, param)

    _task, _param = next(scheduler.tasks)
    assert isinstance(_task, Task)
    assert _param == param
    assert _task.state == State.QUEUED


@given(param=one_of([text(min_size=1), integers()]),)
def test_scheduler_add_task_accepts_task(param):
    scheduler = SerialScheduler()
    task = Task(some_func)
    scheduler.add_task(task, param)

    _task, _param = next(scheduler.tasks)
    assert isinstance(_task, Task)
    assert task == _task
    assert _param == param
    assert _task.state == State.QUEUED


@given(param=one_of([text(min_size=1), integers()]),)
def test_serial_scheduler_returns_success_result(param):

    expected_result = some_func(param)

    scheduler = SerialScheduler()
    task = Task(some_func)
    scheduler.add_task(task, param)
    assert task.state == State.QUEUED

    results = scheduler.results()
    _result = next(results)

    assert task.state == State.SUCCESS
    assert _result == expected_result


@given(param=one_of([text(min_size=1), integers()]),)
def test_thread_pool_scheduler_returns_success_result(param):
    expected_result = some_func(param)

    scheduler = ThreadPoolScheduler()
    task = Task(some_func)
    scheduler.add_task(task, param)
    assert task.state == State.QUEUED

    results = scheduler.results()
    _result = next(results)

    assert task.state == State.SUCCESS
    assert _result == expected_result


@given(param=one_of([text(min_size=1), integers()]),)
def test_serial_scheduler_returns_error_result(param):
    def error_func(x):
        raise Exception

    scheduler = SerialScheduler()
    task = Task(error_func)
    scheduler.add_task(task, param)
    assert task.state == State.QUEUED

    results = scheduler.results()
    _result = next(results)

    assert task.state == State.FAILED
    assert _result is None


@given(param=one_of([text(min_size=1), integers()]),)
def test_thread_pool_scheduler_returns_error_result(param):
    def error_func(x):
        raise Exception

    scheduler = ThreadPoolScheduler()
    task = Task(error_func)
    scheduler.add_task(task, param)
    assert task.state == State.QUEUED

    results = scheduler.results()
    _result = next(results)

    assert task.state == State.FAILED
    assert _result is None
