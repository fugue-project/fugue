from typing import Dict, List, Optional, Tuple

import dask.dataframe as pd
import numpy as np
import pandas
import pyarrow as pa
from dask.distributed import Client, get_client
from triad import assert_or_throw
from triad.utils.pandas_like import PandasLikeUtils
from triad.utils.pyarrow import to_pandas_dtype, to_single_pandas_dtype

import fugue.api as fa
from fugue.constants import FUGUE_CONF_DEFAULT_PARTITIONS

from ._constants import FUGUE_DASK_CONF_DEFAULT_PARTITIONS

_ANTI_INDICATOR = "__anti_indicator__"
_CROSS_INDICATOR = "__corss_indicator__"


def get_default_partitions() -> int:
    conf = fa.get_current_conf()
    n = conf.get(
        FUGUE_DASK_CONF_DEFAULT_PARTITIONS,
        conf.get(FUGUE_CONF_DEFAULT_PARTITIONS, -1),
    )
    return n if n > 0 else fa.get_current_parallelism() * 2


class DaskUtils(PandasLikeUtils[pd.DataFrame]):
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

    def parse_join_type(self, join_type: str) -> str:
        join_type = join_type.replace(" ", "").replace("_", "").lower()
        if join_type in ["inner", "cross"]:
            return join_type
        if join_type in ["inner", "join"]:
            return "inner"
        if join_type in ["semi", "leftsemi"]:
            return "left_semi"
        if join_type in ["anti", "leftanti"]:
            return "left_anti"
        if join_type in ["left", "leftouter"]:
            return "left_outer"
        if join_type in ["right", "rightouter"]:
            return "right_outer"
        if join_type in ["outer", "full", "fullouter"]:
            return "full_outer"
        raise NotImplementedError(join_type)

    def drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates()

    def union(
        self, ndf1: pd.DataFrame, ndf2: pd.DataFrame, unique: bool
    ) -> pd.DataFrame:
        ndf1, ndf2 = self._preprocess_set_op(ndf1, ndf2)
        ndf = ndf1.append(ndf2)
        if unique:
            ndf = self.drop_duplicates(ndf)
        return ndf

    def intersect(
        self, df1: pd.DataFrame, df2: pd.DataFrame, unique: bool
    ) -> pd.DataFrame:
        ndf1, ndf2 = self._preprocess_set_op(df1, df2)
        ndf = ndf1.merge(self.drop_duplicates(ndf2))
        if unique:
            ndf = self.drop_duplicates(ndf)
        return ndf

    def except_df(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        unique: bool,
        anti_indicator_col: str = _ANTI_INDICATOR,
    ) -> pd.DataFrame:
        ndf1, ndf2 = self._preprocess_set_op(df1, df2)
        # TODO: lack of test to make sure original ndf2 is not changed
        ndf2 = self._with_indicator(ndf2, anti_indicator_col)
        ndf = ndf1.merge(ndf2, how="left", on=list(ndf1.columns))
        ndf = ndf[ndf[anti_indicator_col].isnull()].drop([anti_indicator_col], axis=1)
        if unique:
            ndf = self.drop_duplicates(ndf)
        return ndf

    def join(
        self,
        ndf1: pd.DataFrame,
        ndf2: pd.DataFrame,
        join_type: str,
        on: List[str],
        anti_indicator_col: str = _ANTI_INDICATOR,
        cross_indicator_col: str = _CROSS_INDICATOR,
    ) -> pd.DataFrame:
        join_type = self.parse_join_type(join_type)
        if join_type == "inner":
            ndf1 = ndf1.dropna(subset=on)
            ndf2 = ndf2.dropna(subset=on)
            joined = ndf1.merge(ndf2, how=join_type, on=on)
        elif join_type == "left_semi":
            ndf1 = ndf1.dropna(subset=on)
            ndf2 = self.drop_duplicates(ndf2[on].dropna())
            joined = ndf1.merge(ndf2, how="inner", on=on)
        elif join_type == "left_anti":
            # TODO: lack of test to make sure original ndf2 is not changed
            ndf2 = self.drop_duplicates(ndf2[on].dropna())
            ndf2 = self._with_indicator(ndf2, anti_indicator_col)
            joined = ndf1.merge(ndf2, how="left", on=on)
            joined = joined[joined[anti_indicator_col].isnull()].drop(
                [anti_indicator_col], axis=1
            )
        elif join_type == "left_outer":
            ndf2 = ndf2.dropna(subset=on)
            joined = ndf1.merge(ndf2, how="left", on=on)
        elif join_type == "right_outer":
            ndf1 = ndf1.dropna(subset=on)
            joined = ndf1.merge(ndf2, how="right", on=on)
        elif join_type == "full_outer":
            add: List[str] = []
            for f in on:
                name = f + "_null"
                s1 = ndf1[f].isnull().astype(int)
                ndf1[name] = s1
                s2 = ndf2[f].isnull().astype(int) * 2
                ndf2[name] = s2
                add.append(name)
            joined = ndf1.merge(ndf2, how="outer", on=on + add).drop(add, axis=1)
        elif join_type == "cross":
            assert_or_throw(
                len(on) == 0, ValueError(f"cross join can't have join keys {on}")
            )
            ndf1 = self._with_indicator(ndf1, cross_indicator_col)
            ndf2 = self._with_indicator(ndf2, cross_indicator_col)
            joined = ndf1.merge(ndf2, how="inner", on=[cross_indicator_col]).drop(
                [cross_indicator_col], axis=1
            )
        else:  # pragma: no cover
            raise NotImplementedError(join_type)
        return joined

    def _preprocess_set_op(
        self, ndf1: pd.DataFrame, ndf2: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        assert_or_throw(
            len(list(ndf1.columns)) == len(list(ndf2.columns)),
            ValueError("two dataframes have different number of columns"),
        )
        ndf2.columns = ndf1.columns  # this is SQL behavior
        return ndf1, ndf2

    def _with_indicator(self, df: pd.DataFrame, name: str) -> pd.DataFrame:
        return df.assign(**{name: 1})


DASK_UTILS = DaskUtils()
