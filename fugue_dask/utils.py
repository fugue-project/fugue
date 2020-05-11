import dask.dataframe as pd
import pandas
import pyarrow as pa
from triad.utils.pandas_like import PandasLikeUtils


class DaskUtils(PandasLikeUtils[pd.DataFrame]):
    def enforce_type(
        self, df: pd.DataFrame, schema: pa.Schema, null_safe: bool = False
    ) -> pd.DataFrame:
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
        """
        return super().enforce_type(df, schema, null_safe=False)

    def is_compatile_index(self, df: pd.DataFrame) -> bool:
        """Check whether the datafame is compatible with the operations inside
        this utils collection

        :param df: pandas like dataframe
        :return: if it is compatible
        """
        return isinstance(
            df.index,
            (pandas.RangeIndex, pandas.Int64Index, pandas.UInt64Index, pd.Index),
        ) or self.empty(df)


DASK_UTILS = DaskUtils()
