import dask.dataframe as pd
import pandas
import pyarrow as pa
from qpd_dask.engine import DaskUtils as DaskUtilsBase
from triad.utils.pyarrow import to_pandas_dtype


class DaskUtils(DaskUtilsBase):
    def is_compatile_index(self, df: pd.DataFrame) -> bool:
        """Check whether the datafame is compatible with the operations inside
        this utils collection

        :param df: dask dataframe
        :return: if it is compatible
        """
        return isinstance(
            df.index,
            (pandas.RangeIndex, pandas.Int64Index, pandas.UInt64Index, pd.Index),
        )

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
            if pa.types.is_string(v.type):
                ns = s.isnull()
                s = s.astype(str).mask(ns, None)
            elif pa.types.is_boolean(v.type):
                ns = s.isnull()
                if pandas.api.types.is_string_dtype(s.dtype):
                    try:
                        s = s.str.lower() == "true"
                    except AttributeError:
                        s = s.fillna(0).astype(bool)
                else:
                    s = s.fillna(0).astype(bool)
                s = s.mask(ns, None)
            elif pa.types.is_integer(v.type):
                ns = s.isnull()
                s = s.fillna(0).astype(v.type.to_pandas_dtype()).mask(ns, None)
            elif not pa.types.is_struct(v.type) and not pa.types.is_list(v.type):
                s = s.astype(v.type.to_pandas_dtype())
            df[v.name] = s
        return df


DASK_UTILS = DaskUtils()
