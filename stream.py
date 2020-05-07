import itertools
from typing import List, Set, Callable, Iterator, Iterable, Union, Generator, Any


class Stream:
    def __init__(self, items: Union[Iterator, Iterable, Generator]):
        self._items = iter(items)

    def map(self, func: Callable, scheduler):
        return _MapOperator(func, scheduler, self)

    def filter(self, func: Callable):
        return Stream(filter(func, self))

    def take(self, count: int) -> Iterator:
        return Stream(next(self) for _ in range(count))

    def batch(self, count: int) -> Iterator:
        return _BatchOperator(count, self)

    def concat(self):
        return _ConcatOperator(self)

    def list(self) -> List:
        return list(self)

    def set(self) -> Set:
        return set(self)

    def __iter__(self) -> Iterator:
        return self._items

    def __next__(self) -> Any:
        return next(self.__iter__())


class _ConcatOperator(Stream):
    def __init__(self, parent: "Stream") -> None:
        super().__init__(parent)
        self._parent = parent

    def take(self, count: int) -> Iterator:
        return Stream(next(self) for _ in range(count))

    def __iter__(self):
        for itr in self._parent:
            for sub_itr in itr:
                yield sub_itr


class _MapOperator(Stream):
    def __init__(self, func: Callable, scheduler, parent: "Stream") -> None:
        super().__init__(parent)
        self._parent = parent
        self._scheduler = scheduler
        self._func = func

    def take(self, count):
        for item in self._parent.take(count):
            self._scheduler.add_task(self._func, item)
        return Stream(self._scheduler.results())

    def __iter__(self):
        return map(self._func, self._parent)


class _BatchOperator(Stream):
    def __init__(self, count: int, parent: "Stream") -> None:
        super().__init__(parent)
        self._count = count
        self._parent = parent

    def take(self, count: int) -> Iterator:
        return Stream(next(self) for _ in range(count))

    def __iter__(self):
        while True:
            yield self._parent.take(self._count)
