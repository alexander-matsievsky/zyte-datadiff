import abc
from typing import Any, Generator

from zyte_datadiff.dataset.dataset import Dataset


class DataDiff(abc.ABC):
    class LeftOnly:
        left: dict

    class LeftRightEqual:
        left: dict
        right: dict

    class LeftRightNotEqual:
        diff: dict
        left: dict
        right: dict

    class RightOnly:
        right: dict

    Result = LeftOnly | LeftRightEqual | LeftRightNotEqual | RightOnly

    left: Dataset
    right: Dataset

    def __init__(self, *, left: Dataset):
        self.left = left

    def compare(self, *, right: Dataset) -> Generator[Result, None, None]:
        self.right = right
        # todo: implement the comparing logic
        yield

    @abc.abstractmethod
    def project_key(self, item: dict) -> Any | None:
        raise NotImplemented

    @abc.abstractmethod
    def project_value(self, item: dict) -> dict | None:
        raise NotImplemented
