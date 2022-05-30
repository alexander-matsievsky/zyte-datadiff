import abc
import collections.abc


class Dataset(abc.ABC, collections.abc.Iterable):
    @abc.abstractmethod
    def __iter__(self) -> collections.abc.Iterator:
        raise NotImplementedError
