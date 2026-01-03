from typing import Callable, Iterator, Optional
from utils.batch_generator import BatchGenerator
from utils.execution import *


class ExtractorFactory:

    class _extractor:
        def __init__(
            self,
            iter: Iterator,
            batch_size: int,
            wrapper: Optional[Callable] = None,
        ):
            self.cursor = BatchGenerator(
                generator=iter, batch_size=batch_size, wrapper=wrapper
            )

        def __iter__(self):
            return self._extractor_gen()

        # Generator to extract batches of objects from iterator, possibly wrapped in a function
        @safe_generate(fail_return=None)
        def _extractor_gen(self):
            for element in self.cursor:
                yield element

    def build_extractor(
        self,
        iter: Iterator,
        batch_size: int = 1,
        wrapper: Optional[Callable] = None,
    ):
        if batch_size <= 0:
            raise Exception(f"Batch size must be > 0 (current: {batch_size})")
        return self._extractor(
            iter=iter,
            batch_size=batch_size,
            wrapper=wrapper,
        )
