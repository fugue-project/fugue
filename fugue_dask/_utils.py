from typing import Optional

import dask.dataframe as pd
import pandas
import pyarrow as pa
from qpd_dask.engine import DaskUtils as DaskUtilsBase


class DaskUtils(DaskUtilsBase):
    def as_arrow(
        self, df: pd.DataFrame, schema: Optional[pa.Schema] = None
    ) -> pa.Table:
        """Convert dask dataframe to pyarrow table

        :param df: dask dataframe
        :param schema: if specified, it will be used to construct pyarrow table,
          defaults to None

        :return: pyarrow table
        """
        pdf = df.compute().reset_index(drop=True)
        return pa.Table.from_pandas(
            pdf, schema=schema, preserve_index=False, safe=False
        )

    def enforce_type(
        self, df: pd.DataFrame, schema: pa.Schema, null_safe: bool = False
    ) -> pd.DataFrame:
        """Enforce the dask dataframe to comply with `schema`.

        :param df: dask dataframe
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

        :param df: dask dataframe
        :return: if it is compatible
        """
        return (
            isinstance(
                df.index,
                (pandas.RangeIndex, pandas.Int64Index, pandas.UInt64Index, pd.Index),
            )
            or self.empty(df)
        )


DASK_UTILS = DaskUtils()
