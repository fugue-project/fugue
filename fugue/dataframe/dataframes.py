from typing import Any, Dict, List, Union, Callable

from fugue.dataframe.dataframe import DataFrame
from triad.collections.dict import IndexedOrderedDict
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw


class DataFrames(IndexedOrderedDict[str, DataFrame]):
    """Ordered dictionary of DataFrames. There are two modes: with keys
    and without keys. If without key ``_<n>`` will be used as the key
    for each dataframe, and it will be treated as an array in Fugue framework.

    It's a subclass of dict, so it supports all dict operations. It's
    also ordered, so you can trust the order of keys and values.

    The initialization is flexible

    >>> df1 = ArrayDataFrame([[0]],"a:int")
    >>> df2 = ArrayDataFrame([[1]],"a:int")
    >>> dfs = DataFrames(df1,df2)  # init as [df1, df2]
    >>> assert not dfs.has_key
    >>> assert df1 is dfs[0] and df2 is dfs[1]
    >>> dfs_array = list(dfs.values())
    >>> dfs = DataFrames(a=df1,b=df2)  # init as {a:df1, b:df2}
    >>> assert dfs.has_key
    >>> assert df1 is dfs[0] and df2 is dfs[1]  # order is guaranteed
    >>> df3 = ArrayDataFrame([[1]],"b:int")
    >>> dfs2 = DataFrames(dfs, c=df3)  # {a:df1, b:df2, c:df3}
    >>> dfs2 = DataFrames(dfs, df3)  # invalid, because dfs has key, df3 doesn't
    >>> dfs2 = DataFrames(dict(a=df1,b=df2))  # init as {a:df1, b:df2}
    >>> dfs2 = DataFrames([df1,df2],df3)  # init as [df1, df2, df3]
    """

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
        """If this collection has key (dict-like) or not (list-like)"""
        return self._has_key

    def __setitem__(  # type: ignore
        self, key: str, value: DataFrame, *args: Any, **kwds: Any
    ) -> None:
        assert isinstance(key, str)
        assert_or_throw(
            len(self) == 0 or self.has_key,
            InvalidOperationError("this DataFrames can's have key"),
        )
        assert_or_throw(
            isinstance(value, DataFrame), ValueError(f"{key} has non DataFrame value")
        )
        super().__setitem__(key, value, *args, **kwds)  # type: ignore
        self._has_key = True

    def __getitem__(self, key: Union[str, int]) -> DataFrame:  # type: ignore
        if isinstance(key, int):
            key = self.get_key_by_index(key)
        return super().__getitem__(key)  # type: ignore

    def convert(self, func: Callable[["DataFrame"], DataFrame]) -> "DataFrames":
        """Create another DataFrames with the same structure, but all converted by ``func``

        :return: the new DataFrames

        :Example:

        >>> dfs2 = dfs.convert(lambda df: df.as_local()) # convert all to local
        """
        if self.has_key:
            return DataFrames([(k, func(v)) for k, v in self.items()])
        else:
            return DataFrames([func(v) for v in self.values()])

    def _append(self, value: Any):
        assert_or_throw(
            not self.has_key, InvalidOperationError("this DataFrames must have key")
        )
        assert_or_throw(
            isinstance(value, DataFrame), ValueError(f"{value} is not a DataFrame")
        )
        super().__setitem__("_" + str(len(self)), value)
