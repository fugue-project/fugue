from typing import Any, Dict, List, Union, Callable

from fugue.dataframe.dataframe import DataFrame
from triad.collections.dict import IndexedOrderedDict
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw


class DataFrames(IndexedOrderedDict[str, DataFrame]):
    def __init__(self, *args: Any, **kwargs: Any):  # noqa: C901
        super().__init__()
        self._has_key: bool = False
        for d in args:
            if isinstance(d, DataFrames):
                if d.has_key:
                    for k, v in d.items():
                        self[k] = v
                else:
                    for v in d.values():
                        self._append(v)
            elif isinstance(d, DataFrame):
                self._append(d)
            elif isinstance(d, List):
                for o in d:
                    if isinstance(o, tuple):
                        self[o[0]] = o[1]
                    else:
                        self._append(o)
            elif isinstance(d, Dict):
                self.update(d)
            else:
                raise ValueError(f"{d} is not valid to initialize DataFrames")
        self.update(kwargs)
        self.set_readonly()

    @property
    def has_key(self):
        return self._has_key

    def __setitem__(  # type: ignore
        self, key: str, value: DataFrame, *args: Any, **kwds: Any
    ) -> None:
        assert isinstance(key, str)
        assert_or_throw(
            len(self) == 0 or self.has_key,
            InvalidOperationError(f"this DataFrames can's have key"),
        )
        assert_or_throw(
            isinstance(value, DataFrame), ValueError(f"{key} has non DataFrame value")
        )
        super().__setitem__(key, value, *args, **kwds)  # type: ignore
        self._has_key = True

    def __getitem__(  # type: ignore
        self, key: Union[str, int]
    ) -> DataFrame:
        if isinstance(key, int):
            key = self.get_key_by_index(key)
        return super().__getitem__(key)  # type: ignore

    def convert(self, func: Callable[["DataFrame"], DataFrame]) -> "DataFrames":
        if self.has_key:
            return DataFrames([(k, func(v)) for k, v in self.items()])
        else:
            return DataFrames([func(v) for v in self.values()])

    def _append(self, value: Any):
        assert_or_throw(
            not self.has_key, InvalidOperationError(f"this DataFrames must have key")
        )
        assert_or_throw(
            isinstance(value, DataFrame), ValueError(f"{value} is not a DataFrame")
        )
        super().__setitem__("_" + str(len(self)), value)
