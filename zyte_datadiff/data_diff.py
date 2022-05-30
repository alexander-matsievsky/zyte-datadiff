import abc
import dataclasses
import json
import pathlib
import pickle
import sqlite3
import tempfile
import typing

from zyte_datadiff.dataset.dataset import Dataset


class DataDiff(abc.ABC):
    # todo: implement logging and progress notifications
    @dataclasses.dataclass
    class LeftOnly:
        left: dict

    @dataclasses.dataclass
    class LeftRightEqual:
        left: dict
        right: dict

    @dataclasses.dataclass
    class LeftRightNotEqual:
        diff: dict
        left: dict
        right: dict

    @dataclasses.dataclass
    class RightOnly:
        right: dict

    Result = LeftOnly | LeftRightEqual | LeftRightNotEqual | RightOnly

    left: Dataset
    left_sqlite_path: pathlib.Path
    right: Dataset
    right_sqlite_path: pathlib.Path = None

    def __init__(self, *, left: Dataset):
        self.left = left
        self.left_sqlite_path = self.ingest(self.left)

    def compare(self, *, right: Dataset) -> typing.Generator[Result, None, None]:
        self.right = right
        self.right_sqlite_path = self.ingest(self.right)
        conn = sqlite3.connect(":memory")
        conn.executescript(
            f"""
            -- language=SQLite
            attach {self.left_sqlite_path.as_posix()!r} as left;
            attach {self.right_sqlite_path.as_posix()!r} as right;
            """
        )
        for (
            left_key,
            left_item,
            left_value,
            right_key,
            right_item,
            right_value,
        ) in conn.execute(
            """
            -- language=SQLite
            select left_items.key    as left_key,
                   left_items.item   as left_item,
                   left_items.value  as left_value,
                   right_items.key   as right_key,
                   right_items.item  as right_item,
                   right_items.value as right_value
            from "left".items as left_items
                     left outer join
                 "right".items as right_items
                 using (key)
            union
            select left_items.key    as left_key,
                   left_items.item   as left_item,
                   left_items.value  as left_value,
                   right_items.key   as right_key,
                   right_items.item  as right_item,
                   right_items.value as right_value
            from "right".items as right_items
                     left outer join
                 "left".items as left_items
                 using (key);
            """
        ):
            if right_key is None:
                yield DataDiff.LeftOnly(
                    left=pickle.loads(left_item),
                )
            elif left_key is None:
                yield DataDiff.RightOnly(
                    right=pickle.loads(right_item),
                )
            elif left_value == right_value:
                yield DataDiff.LeftRightEqual(
                    left=pickle.loads(left_item),
                    right=pickle.loads(right_item),
                )
            elif left_value != right_value:
                # todo: define and implement `diff`
                yield DataDiff.LeftRightNotEqual(
                    diff={},
                    left=pickle.loads(left_item),
                    right=pickle.loads(right_item),
                )
            else:
                left_item = pickle.loads(left_item)
                right_item = pickle.loads(right_item)
                raise ValueError(
                    "could not categorise the diff\n\n"
                    + json.dumps(
                        {
                            "left_key": self.project_key(left_item),
                            "left_item": left_item,
                            "left_value": self.project_value(left_item),
                            "right_key": self.project_key(right_item),
                            "right_item": right_item,
                            "right_value": self.project_value(right_item),
                        },
                        indent=2,
                    )
                )

    def ingest(self, dataset: Dataset) -> pathlib.Path:
        with tempfile.NamedTemporaryFile(delete=False) as sqlite:
            sqlite_path = pathlib.Path(sqlite.name)
        conn = sqlite3.connect(sqlite_path)
        conn.executescript(
            """
            -- language=SQLite
            create table items (key integer, item binary, value integer);
            create unique index items_key on items (key);
            pragma journal_mode = off;
            pragma synchronous = off;
            """
        )
        conn.execute("begin")
        for item in dataset:
            conn.execute(
                """
                -- language=SQLite
                insert or ignore into items(key, item, value)
                values (?, ?, ?)
                """,
                (
                    hash(pickle.dumps(self.project_key(item))),
                    pickle.dumps(item),
                    hash(pickle.dumps(self.project_value(item))),
                ),
            )
        conn.execute("end")
        conn.close()
        return sqlite_path

    @abc.abstractmethod
    def project_key(self, item: dict) -> typing.Any | None:
        raise NotImplemented

    @abc.abstractmethod
    def project_value(self, item: dict) -> dict | None:
        raise NotImplemented
