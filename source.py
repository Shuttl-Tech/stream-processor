import itertools
from typing import List, Set, Callable, Iterator, Iterable, Union, Generator, Any


class Source:
    def __init__(self, items: Union[Iterator, Iterable, Generator]):
        self._items = iter(items)

    def map(self, func: Callable, scheduler):
        return _MapOperator(func, scheduler, self)

    def filter(self, func: Callable):
        return Source(filter(func, self))

    def take(self, count: int) -> Iterator:
        return Source(next(self) for _ in range(count))

    def batch(self, count: int) -> Iterator:
        return _BatchOperator(count, self)

    def concat(self):
        return Source(itertools.chain(self))

    def list(self) -> List:
        return list(self)

    def set(self) -> Set:
        return set(self)

    def __iter__(self) -> Iterator:
        return self._items

    def __next__(self) -> Any:
        return next(self.__iter__())


class _MapOperator(Source):
    def __init__(self, func: Callable, scheduler, parent: "Source") -> None:
        super().__init__(parent)
        self._parent = parent
        self._scheduler = scheduler
        self._func = func

    def take(self, count):
        self._scheduler.add_tasks(
            (self._func, task) for task in self._parent.take(count)
        )
        return self._scheduler.results()

    def __iter__(self):
        return map(self._func, self._parent)


class _BatchOperator(Source):
    def __init__(self, count: int, parent: "Source") -> None:
        super().__init__(parent)
        self._count = count
        self._parent = parent

    def __iter__(self):
        while True:
            yield self._parent.take(self._count)
