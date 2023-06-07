from typing import Dict, Optional

import dask.dataframe as pd
import numpy as np
import pandas
import pyarrow as pa
from dask.distributed import Client, get_client
from triad.utils.pandas_like import PandasLikeUtils
from triad.utils.pyarrow import to_pandas_dtype, to_single_pandas_dtype

import fugue.api as fa
from fugue.constants import FUGUE_CONF_DEFAULT_PARTITIONS

from ._constants import FUGUE_DASK_CONF_DEFAULT_PARTITIONS


def get_default_partitions() -> int:
    conf = fa.get_current_conf()
    n = conf.get(
        FUGUE_DASK_CONF_DEFAULT_PARTITIONS,
        conf.get(FUGUE_CONF_DEFAULT_PARTITIONS, -1),
    )
    return n if n > 0 else fa.get_current_parallelism() * 2


class DaskUtils(PandasLikeUtils[pd.DataFrame, pd.Series]):
    def concat_dfs(self, *dfs: pd.DataFrame) -> pd.DataFrame:
        return pd.concat(list(dfs))

    def get_or_create_client(self, client: Optional[Client] = None):
        if client is not None:
            return client
        try:
            return get_client()
        except ValueError:
            return Client(processes=True)

    def is_compatile_index(self, df: pd.DataFrame) -> bool:
        """Check whether the datafame is compatible with the operations inside
        this utils collection

        :param df: dask dataframe
        :return: if it is compatible
        """
        if isinstance(df.index, pd.Index):
            return True
        return isinstance(df.index, pandas.RangeIndex) or (  # pragma: no cover
            hasattr(df.index, "inferred_type") and df.index.inferred_type == "integer"
        )

    def safe_to_pandas_dtype(self, schema: pa.Schema) -> Dict[str, np.dtype]:
        """Safely convert pyarrow types to pandas types. It will convert nested types
        to numpy object type. And this does not convert to pandas extension types.

        :param schema: the input pyarrow schema
        :return: the dictionary of numpy types

        .. note::

            This is a temporary solution, it will be removed when we use the Slide
            package. Do not use this function directly.
        """
        res: Dict[str, np.dtype] = {}
        for f in schema:
            if pa.types.is_nested(f.type):
                res[f.name] = np.dtype(object)
            else:
                res[f.name] = to_single_pandas_dtype(f.type, use_extension_types=False)
        return res

    # TODO: merge this back to base class
    def enforce_type(  # noqa: C901
        self, df: pd.DataFrame, schema: pa.Schema, null_safe: bool = False
    ) -> pd.DataFrame:  # pragma: no cover
        """Enforce the pandas like dataframe to comply with `schema`.
        :param df: pandas like dataframe
        :param schema: pyarrow schema
        :param null_safe: whether to enforce None value for int, string and bool values
        :return: converted dataframe
        :Notice:
        When `null_safe` is true, the native column types in the dataframe may change,
        for example, if a column of `int64` has None values, the output will make sure
        each value in the column is either None or an integer, however, due to the
        behavior of pandas like dataframes, the type of the columns may
        no longer be `int64`
        This method does not enforce struct and list types
        """
        if not null_safe:
            return df.astype(dtype=to_pandas_dtype(schema))
        for v in schema:
            s = df[v.name]
            if pa.types.is_string(v.type) and not pandas.api.types.is_string_dtype(
                s.dtype
            ):
                ns = s.isnull()
                s = s.astype(str).mask(ns, None)
            elif pa.types.is_boolean(v.type) and not pandas.api.types.is_bool_dtype(
                s.dtype
            ):
                ns = s.isnull()
                if pandas.api.types.is_string_dtype(s.dtype):
                    try:
                        s = s.str.lower() == "true"
                    except AttributeError:
                        s = s.fillna(0).astype(bool)
                else:
                    s = s.fillna(0).astype(bool)
                s = s.mask(ns, None).astype("boolean")
            elif pa.types.is_integer(v.type) and not pandas.api.types.is_integer_dtype(
                s.dtype
            ):
                ns = s.isnull()
                s = s.fillna(0).astype(v.type.to_pandas_dtype()).mask(ns, None)
            elif not pa.types.is_struct(v.type) and not pa.types.is_list(v.type):
                s = s.astype(v.type.to_pandas_dtype())
            df[v.name] = s
        return df


DASK_UTILS = DaskUtils()
