import math
from typing import Any, Callable, List, Optional, Tuple, TypeVar

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pyarrow as pa
from dask.dataframe import DataFrame
from dask.delayed import delayed
from dask.distributed import Client, get_client
from triad.utils.pandas_like import PD_UTILS, PandasLikeUtils
from triad.utils.pyarrow import to_pandas_dtype

import fugue.api as fa
from fugue.constants import FUGUE_CONF_DEFAULT_PARTITIONS

from ._constants import FUGUE_DASK_CONF_DEFAULT_PARTITIONS

_FUGUE_DASK_TEMP_IDX_COLUMN = "_fugue_dask_temp_index"
T = TypeVar("T")


def get_default_partitions() -> int:
    """Get the default number of partitions"""
    conf = fa.get_current_conf()
    n = conf.get(
        FUGUE_DASK_CONF_DEFAULT_PARTITIONS,
        conf.get(FUGUE_CONF_DEFAULT_PARTITIONS, -1),
    )
    return n if n > 0 else fa.get_current_parallelism() * 2


def collect(df: dd.DataFrame, func: Callable[[pd.DataFrame], T]) -> Tuple[T]:
    """Compute each partition in parallel and collect the results

    :param df: dask dataframe
    :return: the collected result
    """
    dfs = df.to_delayed()
    objs = [delayed(func)(df) for df in dfs]
    return dd.compute(*objs)


def hash_repartition(df: dd.DataFrame, num: int, cols: List[Any]) -> dd.DataFrame:
    """Repartition the dataframe by hashing the given columns

    :param df: dataframe to repartition
    :param num: number of partitions
    :param cols: columns to hash, if empty, all columns will be used

    :return: repartitioned dataframe
    """
    if num < 1:
        return df
    if num == 1:
        return df.repartition(npartitions=1)
    df = df.reset_index(drop=True).clear_divisions()
    idf, ct = _add_hash_index(df, num, cols)
    return _postprocess(idf, ct, num)


def even_repartition(df: dd.DataFrame, num: int, cols: List[Any]) -> dd.DataFrame:
    """Repartition the dataframe by evenly distributing the given columns

    :param df: dataframe to repartition
    :param num: number of partitions
    :param cols: group columns

    :return: repartitioned dataframe

    .. note::

        When cols is empty, the dataframe will be evenly repartitioned by ``num``.
        When cols is not empty, the dataframe will be evenly repartitioned by the
        groups of the given columns. When cols is not empty, if ``num<=0``,
        the number of partitions will be the number of groups.
    """
    if num == 1:
        return df.repartition(npartitions=1)
    if len(cols) == 0 and num <= 0:
        return df
    df = df.reset_index(drop=True).clear_divisions()
    if len(cols) == 0:
        idf, ct = _add_continuous_index(df)
    else:
        idf, ct = _add_group_index(df, cols, shuffle=False)
        # when cols are set and num is not set, we use the number of groups
        if num <= 0:
            num = ct
    return _postprocess(idf, ct, num)


def rand_repartition(
    df: dd.DataFrame, num: int, cols: List[Any], seed: Any = None
) -> dd.DataFrame:
    """Repartition the dataframe by randomly distributing the rows or groups

    :param df: dataframe to repartition
    :param num: number of partitions
    :param cols: group columns
    :param seed: random seed

    :return: repartitioned dataframe

    .. note::

        When cols is empty, the dataframe will be randomly shuffled and
        repartitioned by ``num``. When cols is not empty, the dataframe will be
        grouped and then randomly shuffled by groups.
    """
    if num < 1:
        return df
    if num == 1:
        return df.repartition(npartitions=1)
    df = df.reset_index(drop=True).clear_divisions()
    if len(cols) == 0:
        idf, ct = _add_random_index(df, num=num, seed=seed)
    else:
        idf, ct = _add_group_index(df, cols, shuffle=True, seed=seed)
        # when cols are set and num is not set, we use the number of groups
    return _postprocess(idf, ct, num)


def _postprocess(idf: dd.DataFrame, ct: int, num: int) -> dd.DataFrame:
    parts = min(ct, num)
    if parts <= 1:
        return idf.repartition(npartitions=1)
    divisions = list(np.arange(ct, step=math.ceil(ct / parts)))
    divisions.append(ct - 1)
    return idf.repartition(divisions=divisions, force=True)


def _add_group_index(
    df: dd.DataFrame, cols: List[str], shuffle: bool, seed: Any = None
) -> Tuple[dd.DataFrame, int]:
    keys = df[cols].drop_duplicates().compute()
    if shuffle:
        keys = keys.sample(frac=1, random_state=seed)
    keys = keys.reset_index(drop=True).assign(
        **{_FUGUE_DASK_TEMP_IDX_COLUMN: pd.Series(range(len(keys)), dtype=int)}
    )
    df = df.merge(dd.from_pandas(keys, npartitions=1), on=cols, broadcast=True)
    return df.set_index(_FUGUE_DASK_TEMP_IDX_COLUMN, drop=True), len(keys)


def _add_hash_index(
    df: dd.DataFrame, num: int, cols: List[str]
) -> Tuple[dd.DataFrame, int]:
    if len(cols) == 0:
        cols = list(df.columns)

    def _add_hash(df: pd.DataFrame) -> pd.DataFrame:  # pragma: no cover
        if len(df) == 0:
            return df.assign(**{_FUGUE_DASK_TEMP_IDX_COLUMN: pd.Series(dtype=int)})
        return df.assign(
            **{
                _FUGUE_DASK_TEMP_IDX_COLUMN: pd.util.hash_pandas_object(
                    df[cols], index=False
                )
                .mod(num)
                .astype(int)
            }
        )

    orig_schema = list(df.dtypes.to_dict().items())
    idf = df.map_partitions(
        _add_hash, meta=orig_schema + [(_FUGUE_DASK_TEMP_IDX_COLUMN, int)]
    ).set_index(_FUGUE_DASK_TEMP_IDX_COLUMN, drop=True)
    return idf, num


def _add_random_index(
    df: dd.DataFrame, num: int, seed: Any = None
) -> Tuple[dd.DataFrame, int]:  # pragma: no cover
    def _add_rand(df: pd.DataFrame) -> pd.DataFrame:
        if len(df) == 0:
            return df.assign(**{_FUGUE_DASK_TEMP_IDX_COLUMN: pd.Series(dtype=int)})
        if seed is not None:
            np.random.seed(seed)
        return df.assign(
            **{_FUGUE_DASK_TEMP_IDX_COLUMN: np.random.randint(0, num, len(df))}
        )

    orig_schema = list(df.dtypes.to_dict().items())
    idf = df.map_partitions(
        _add_rand, meta=orig_schema + [(_FUGUE_DASK_TEMP_IDX_COLUMN, int)]
    ).set_index(_FUGUE_DASK_TEMP_IDX_COLUMN, drop=True)
    return idf, num


def _add_continuous_index(df: dd.DataFrame) -> Tuple[dd.DataFrame, int]:
    def _get_info(
        df: pd.DataFrame, partition_info: Any
    ) -> pd.DataFrame:  # pragma: no cover
        return pd.DataFrame(dict(no=[partition_info["number"]], ct=[len(df)]))

    pinfo = (
        df.index.to_frame(name=df.index.name)
        .map_partitions(_get_info, meta={"no": int, "ct": int})
        .compute()
    )
    counts = pinfo.sort_values("no").ct.cumsum().tolist()
    starts = [0] + counts[0:-1]

    def _add_index(
        df: pd.DataFrame, partition_info: Any
    ) -> pd.DataFrame:  # pragma: no cover
        return df.assign(
            **{
                _FUGUE_DASK_TEMP_IDX_COLUMN: np.arange(len(df))
                + starts[partition_info["number"]]
            }
        )

    orig_schema = list(df.dtypes.to_dict().items())
    idf = df.map_partitions(
        _add_index, meta=orig_schema + [(_FUGUE_DASK_TEMP_IDX_COLUMN, int)]
    )
    idf = idf.set_index(_FUGUE_DASK_TEMP_IDX_COLUMN, drop=True)
    return idf, counts[-1]


class DaskUtils(PandasLikeUtils[dd.DataFrame, dd.Series]):
    def concat_dfs(self, *dfs: dd.DataFrame) -> dd.DataFrame:
        return dd.concat(list(dfs))

    def get_or_create_client(self, client: Optional[Client] = None):
        if client is not None:
            return client
        try:
            return get_client()
        except ValueError:  # pragma: no cover
            return Client(processes=True)

    def ensure_compatible(self, df: DataFrame) -> None:
        if df.index.name != _FUGUE_DASK_TEMP_IDX_COLUMN:
            super().ensure_compatible(df)

    def is_compatile_index(self, df: dd.DataFrame) -> bool:
        """Check whether the datafame is compatible with the operations inside
        this utils collection

        :param df: dask dataframe
        :return: if it is compatible
        """
        if isinstance(df.index, dd.Index):
            return True
        return isinstance(df.index, pd.RangeIndex) or (  # pragma: no cover
            hasattr(df.index, "inferred_type") and df.index.inferred_type == "integer"
        )

    def cast_df(
        self,
        df: DataFrame,
        schema: pa.Schema,
        use_extension_types: bool = True,
        use_arrow_dtype: bool = False,
        **kwargs: Any
    ) -> DataFrame:
        output_dtypes = to_pandas_dtype(
            schema,
            use_extension_types=use_extension_types,
            use_arrow_dtype=use_arrow_dtype,
        )
        return df.map_partitions(
            PD_UTILS.cast_df,
            schema=schema,
            use_extension_types=use_extension_types,
            use_arrow_dtype=use_arrow_dtype,
            meta=output_dtypes,
            **kwargs
        )


DASK_UTILS = DaskUtils()
