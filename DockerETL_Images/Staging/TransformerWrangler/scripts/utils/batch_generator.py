from typing import TypeVar, Generator, Iterator, Callable, Optional, Union

T = TypeVar("T")  # generic type for items
R = TypeVar("R")  # return type of the optional function


class BatchGenerator:
    def __init__(
        self,
        generator: Generator[T, None, None] | Iterator[T],
        batch_size: int,
        filter_func: Optional[Callable[[T], bool]] = None,
        wrapper: Optional[Callable[[T], R]] = None
    ):
        # Generator taht takes a generator like ijson.kvitems(...) or csv reader
        # Takes a batch size
        # takes a filter func that returns a bool

        # The filter func lets us define whatever func we want, it takes the generator:generator object
        # and it returns true (we filter, not added to batch) or false (we add to batch, not filtered) wheter we add it to the batch lsit or not.
        # Wrapper is a function that lets you decide what operation to do on each item before adding it to the batch (e.g. wrapping it in an ORM model)
        self.generator = generator
        self.batch_size = batch_size
        self.filter_func = filter_func
        self.wrapper = wrapper
        self.completed = False

    def __iter__(self):
        return self

    # @safe_execute(fail_return=None)
    def __next__(self) -> list[T | R]:
        if self.completed:
            raise StopIteration

        batch: list[T] = []
        for item in self.generator:
            if self.filter_func and not self.filter_func(item):
                continue
            if self.wrapper: 
                item = self.wrapper(item)
            batch.append(item)
            if len(batch) >= self.batch_size:
                return batch
        # Si le générateur est fini mais un batch partiel existe -> on le renvoie
        if batch:
            self.completed = True
            return batch

        self.completed = True
        raise StopIteration
