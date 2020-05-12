import itertools
from copy import deepcopy
from typing import List, Set, Callable, Iterator, Iterable, Union, Generator, Any

from shuttl_workflows.schedulers import Scheduler, SerialScheduler


class Stream:
    def __init__(self, items: Union[Iterator, Iterable, Generator]):
        self._items = iter(items)

    def map(self, func: Callable, scheduler: "Scheduler" = None) -> "Stream":
        return _MapOperator(func, self, scheduler)

    def filter(self, func: Callable) -> "Stream":
        return Stream(filter(deepcopy(func), self))

    def take(self, count: int) -> "Stream":
        return Stream(itertools.islice(self, count))

    def batch(self, count: int) -> "Stream":
        return _BatchOperator(count, self)

    def concat(self) -> "Stream":
        return _ConcatOperator(self)

    def list(self) -> List:
        return list(self)

    def set(self) -> Set:
        return set(self)

    def __iter__(self):
        return self._items

    def __next__(self) -> Any:
        return next(self.__iter__())


class _ConcatOperator(Stream):
    def __init__(self, parent: "Stream") -> None:
        super().__init__(parent)
        self._parent = parent

    def take(self, count: int) -> Iterator:
        return Stream(itertools.islice(self, count))

    def __iter__(self):
        for itr in self._parent:
            for sub_itr in itr:
                yield sub_itr


class _MapOperator(Stream):
    def __init__(
        self, func: Callable, parent: "Stream", scheduler: "Scheduler"
    ) -> None:
        super().__init__(parent)
        self._parent = parent
        self._scheduler = scheduler or SerialScheduler()
        self._func = func

    def take(self, count) -> "Stream":
        for item in self._parent.take(count):
            self._scheduler.add_task(deepcopy(self._func), item)
        return Stream(self._scheduler.results())

    def __iter__(self):
        return map(deepcopy(self._func), self._parent)


class _BatchOperator(Stream):
    def __init__(self, count: int, parent: "Stream") -> None:
        super().__init__(parent)
        self._count = count
        self._parent = parent

    def take(self, count: int) -> "Stream":
        return Stream(itertools.islice(self, count))

    def __iter__(self):
        while True:
            batch = self._parent.take(self._count).list()
            if not batch:
                return
            yield batch
