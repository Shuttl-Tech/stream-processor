from hypothesis import given
from hypothesis.strategies import integers, lists

from stream_processor.stream import Stream


def some_func(x):
    return x * 2


@given(data=lists(elements=integers(), max_size=100))
def test_map(data):
    class _FakeScheduler:
        pass

    stream = iter(data)
    expected_result = list(map(some_func, data))

    scheduler = _FakeScheduler()
    result = Stream(stream).map(some_func, scheduler=scheduler)
    assert expected_result == list(result)


@given(data=lists(elements=integers(), max_size=100))
def test_filter(data):
    def _filter(x):
        return x % 2 == 0

    stream = iter(data)
    expected_result = list(filter(_filter, data))

    result = Stream(stream).filter(_filter)
    assert expected_result == list(result)


@given(data=lists(elements=integers(), max_size=100), count=integers(1, 100))
def test_take(data, count):
    stream = iter(data)
    result = Stream(stream).take(count)
    assert data[:count] == list(result)


@given(count=integers(1, 100))
def test_batch(count):

    data = list(range(100))

    stream = iter(data)
    result = Stream(stream).batch(count)

    for i in range(0, 100, count):
        assert data[i : i + count] == list(next(result))


@given(data=lists(elements=integers(), max_size=1000))
def test_concat(data):

    chunk_size = 10

    def _batch(_data):
        chunk = "0"
        while chunk:
            chunk = _data[:chunk_size]
            _data = _data[chunk_size:]
            yield chunk

    result = Stream(_batch(data)).concat()
    assert data == list(result)


@given(count=integers(1, 100))
def test_batch_with_concat_stream(count):

    data = list(range(100))

    stream = iter(data)
    result = Stream(stream).batch(count).concat()

    assert data == list(result)


@given(count=integers(1, 100))
def test_take_with_map_stream(count):

    data = list(range(100))

    class _FakeScheduler:
        @staticmethod
        def add_task(func, *args, **kwargs):
            assert func == some_func

        @staticmethod
        def results():
            return map(some_func, data[:count])

    expected_result = list(map(some_func, data[:count]))

    scheduler = _FakeScheduler()
    result = Stream(data).map(some_func, scheduler=scheduler).take(count)
    assert expected_result == list(result)
