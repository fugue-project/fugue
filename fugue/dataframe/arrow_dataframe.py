from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow as pa
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.utils.pyarrow import cast_pa_table, pa_table_to_pandas

from fugue.dataset.api import (
    as_fugue_dataset,
    as_local,
    as_local_bounded,
    count,
    get_num_partitions,
    is_bounded,
    is_empty,
    is_local,
)
from fugue.exceptions import FugueDataFrameOperationError

from .api import (
    alter_columns,
    as_array,
    as_array_iterable,
    as_dict_iterable,
    as_dicts,
    as_pandas,
    drop_columns,
    get_column_names,
    get_schema,
    is_df,
    rename,
    select_columns,
)
from .dataframe import DataFrame, LocalBoundedDataFrame, _input_schema
from .utils import (
    pa_table_as_array,
    pa_table_as_array_iterable,
    pa_table_as_dict_iterable,
    pa_table_as_dicts,
)


class ArrowDataFrame(LocalBoundedDataFrame):
    """DataFrame that wraps :func:`pyarrow.Table <pa:pyarrow.table>`. Please also read
    |DataFrameTutorial| to understand this Fugue concept

    :param df: 2-dimensional array, iterable of arrays,
      :func:`pyarrow.Table <pa:pyarrow.table>` or pandas DataFrame
    :param schema: |SchemaLikeObject|

    .. admonition:: Examples

        >>> ArrowDataFrame([[0,'a'],[1,'b']],"a:int,b:str")
        >>> ArrowDataFrame(schema = "a:int,b:int")  # empty dataframe
        >>> ArrowDataFrame(pd.DataFrame([[0]],columns=["a"]))
        >>> ArrowDataFrame(ArrayDataFrame([[0]],"a:int).as_arrow())
    """

    def __init__(  # noqa: C901
        self,
        df: Any = None,
        schema: Any = None,
    ):
        if df is None:
            schema = _input_schema(schema).assert_not_empty()
            self._native: pa.Table = schema.create_empty_arrow_table()
            super().__init__(schema)
            return
        elif isinstance(df, pa.Table):
            assert_or_throw(
                schema is None,
                InvalidOperationError("can't reset schema for pa.Table"),
            )
            self._native = df
            super().__init__(Schema(df.schema))
            return
        elif isinstance(df, (pd.DataFrame, pd.Series)):
            if isinstance(df, pd.Series):
                df = df.to_frame()
            pdf = df
            if schema is None:
                self._native = pa.Table.from_pandas(
                    pdf,
                    schema=Schema(pdf).pa_schema,
                    preserve_index=False,
                    safe=True,
                )
                schema = Schema(self._native.schema)
            else:
                schema = _input_schema(schema).assert_not_empty()
                if pdf.shape[0] == 0:
                    self._native = schema.create_empty_arrow_table()
                else:
                    self._native = pa.Table.from_pandas(
                        pdf,
                        schema=schema.pa_schema,
                        preserve_index=False,
                        safe=True,
                    )
            super().__init__(schema)
            return
        elif isinstance(df, Iterable):
            schema = _input_schema(schema).assert_not_empty()
            pdf = pd.DataFrame(df, columns=schema.names)
            if pdf.shape[0] == 0:
                self._native = schema.create_empty_arrow_table()
            else:
                for f in schema.fields:
                    if pa.types.is_timestamp(f.type) or pa.types.is_date(f.type):
                        pdf[f.name] = pd.to_datetime(pdf[f.name])
                schema = _input_schema(schema).assert_not_empty()
                self._native = pa.Table.from_pandas(
                    pdf, schema=schema.pa_schema, preserve_index=False, safe=True
                )
            super().__init__(schema)
            return
        else:
            raise ValueError(f"{df} is incompatible with ArrowDataFrame")

    @property
    def native(self) -> pa.Table:
        """:func:`pyarrow.Table <pa:pyarrow.table>`"""
        return self._native

    def native_as_df(self) -> pa.Table:
        return self._native

    @property
    def empty(self) -> bool:
        return self.count() == 0

    def peek_array(self) -> List[Any]:
        self.assert_not_empty()
        data = self.native.take([0]).to_pydict()
        return [v[0] for v in data.values()]

    def peek_dict(self) -> Dict[str, Any]:
        self.assert_not_empty()
        data = self.native.take([0]).to_pydict()
        return {k: v[0] for k, v in data.items()}

    def count(self) -> int:
        return self.native.shape[0]

    def as_pandas(self) -> pd.DataFrame:
        return _pa_table_as_pandas(self.native)

    def head(
        self, n: int, columns: Optional[List[str]] = None
    ) -> LocalBoundedDataFrame:
        adf = self.native if columns is None else self.native.select(columns)
        n = min(n, self.count())
        if n == 0:
            schema = self.schema if columns is None else self.schema.extract(columns)
            return ArrowDataFrame(None, schema=schema)
        return ArrowDataFrame(adf.take(list(range(n))))

    def _drop_cols(self, cols: List[str]) -> DataFrame:
        return ArrowDataFrame(self.native.drop(cols))

    def _select_cols(self, keys: List[Any]) -> DataFrame:
        return ArrowDataFrame(self.native.select(keys))

    def rename(self, columns: Dict[str, str]) -> DataFrame:
        try:
            cols = dict(columns)
            new_cols = [cols.pop(c, c) for c in self.columns]
            assert_or_throw(len(cols) == 0)
        except Exception as e:
            raise FugueDataFrameOperationError from e
        return ArrowDataFrame(self.native.rename_columns(new_cols))

    def alter_columns(self, columns: Any) -> DataFrame:
        adf = _pa_table_alter_columns(self.native, columns)
        if adf is self.native:
            return self
        return ArrowDataFrame(adf)

    def as_arrow(self, type_safe: bool = False) -> pa.Table:
        return self.native

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        return pa_table_as_array(self.native, columns=columns)

    def as_dicts(self, columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        return pa_table_as_dicts(self.native, columns=columns)

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        yield from pa_table_as_array_iterable(self.native, columns=columns)

    def as_dict_iterable(
        self, columns: Optional[List[str]] = None
    ) -> Iterable[Dict[str, Any]]:
        yield from pa_table_as_dict_iterable(self.native, columns=columns)


@as_local.candidate(lambda df: isinstance(df, pa.Table))
def _pa_table_as_local(df: pa.Table) -> pa.Table:
    return df


@as_local_bounded.candidate(lambda df: isinstance(df, pa.Table))
def _pa_table_as_local_bounded(df: pa.Table) -> pa.Table:
    return df


@as_pandas.candidate(lambda df: isinstance(df, pa.Table))
def _pa_table_as_pandas(df: pa.Table) -> pd.DataFrame:
    return pa_table_to_pandas(
        df,
        use_extension_types=True,
        use_arrow_dtype=False,
        use_threads=False,
        date_as_object=False,
    )


@as_array.candidate(lambda df, *args, **kwargs: isinstance(df, pa.Table))
def _pa_table_as_array(
    df: pa.Table, columns: Optional[List[str]] = None, type_safe: bool = False
) -> List[Any]:
    return pa_table_as_array(df, columns=columns)


@as_array_iterable.candidate(lambda df, *args, **kwargs: isinstance(df, pa.Table))
def _pa_table_as_array_iterable(
    df: pa.Table, columns: Optional[List[str]] = None, type_safe: bool = False
) -> Iterable[Any]:
    yield from pa_table_as_array_iterable(df, columns=columns)


@as_dicts.candidate(lambda df, *args, **kwargs: isinstance(df, pa.Table))
def _pa_table_as_dicts(
    df: pa.Table, columns: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    return pa_table_as_dicts(df, columns=columns)


@as_dict_iterable.candidate(lambda df, *args, **kwargs: isinstance(df, pa.Table))
def _pa_table_as_dict_iterable(
    df: pa.Table, columns: Optional[List[str]] = None
) -> Iterable[Dict[str, Any]]:
    yield from pa_table_as_dict_iterable(df, columns=columns)


@alter_columns.candidate(lambda df, *args, **kwargs: isinstance(df, pa.Table))
def _pa_table_alter_columns(
    df: pa.Table, columns: Any, as_fugue: bool = False
) -> pa.Table:
    schema = Schema(df.schema)
    new_schema = schema.alter(columns)
    if schema != new_schema:
        df = cast_pa_table(df, new_schema.pa_schema)
    return df if not as_fugue else ArrowDataFrame(df)


@as_fugue_dataset.candidate(lambda df, **kwargs: isinstance(df, pa.Table))
def _pa_table_as_fugue_df(df: pa.Table, **kwargs: Any) -> "ArrowDataFrame":
    return ArrowDataFrame(df, **kwargs)


@is_df.candidate(lambda df: isinstance(df, pa.Table))
def _pa_table_is_df(df: pa.Table) -> bool:
    return True


@count.candidate(lambda df: isinstance(df, pa.Table))
def _pa_table_count(df: pa.Table) -> int:
    return df.shape[0]


@is_bounded.candidate(lambda df: isinstance(df, pa.Table))
def _pa_table_is_bounded(df: pa.Table) -> bool:
    return True


@is_empty.candidate(lambda df: isinstance(df, pa.Table))
def _pa_table_is_empty(df: pa.Table) -> bool:
    return df.shape[0] == 0


@is_local.candidate(lambda df: isinstance(df, pa.Table))
def _pa_table_is_local(df: pa.Table) -> bool:
    return True


@get_num_partitions.candidate(lambda df: isinstance(df, pa.Table))
def _pa_table_get_num_partitions(df: pa.Table) -> int:
    return 1


@get_column_names.candidate(lambda df: isinstance(df, pa.Table))
def _get_pyarrow_table_columns(df: pa.Table) -> List[Any]:
    return [f.name for f in df.schema]


@get_schema.candidate(lambda df: isinstance(df, pa.Table))
def _get_pyarrow_table_schema(df: pa.Table) -> Schema:
    return Schema(df.schema)


@rename.candidate(lambda df, *args, **kwargs: isinstance(df, pa.Table))
def _rename_pyarrow_dataframe(df: pa.Table, columns: Dict[str, Any]) -> pa.Table:
    if len(columns) == 0:
        return df
    _assert_no_missing(df, columns.keys())
    return df.rename_columns([columns.get(f.name, f.name) for f in df.schema])


@drop_columns.candidate(lambda df, *args, **kwargs: isinstance(df, pa.Table))
def _drop_pa_columns(df: pa.Table, columns: List[str]) -> pa.Table:
    cols = [x for x in df.schema.names if x not in columns]
    if len(cols) == 0:
        raise FugueDataFrameOperationError("cannot drop all columns")
    if len(cols) + len(columns) != len(df.columns):
        _assert_no_missing(df, columns)
    return df.select(cols)


@select_columns.candidate(lambda df, *args, **kwargs: isinstance(df, pa.Table))
def _select_pa_columns(df: pa.Table, columns: List[Any]) -> pa.Table:
    if len(columns) == 0:
        raise FugueDataFrameOperationError("must select at least one column")
    _assert_no_missing(df, columns=columns)
    return df.select(columns)


def _build_empty_arrow(schema: Schema) -> pa.Table:  # pragma: no cover
    # TODO: remove
    return schema.create_empty_arrow_table()


def _assert_no_missing(df: pa.Table, columns: Iterable[Any]) -> None:
    missing = [x for x in columns if x not in df.schema.names]
    if len(missing) > 0:
        raise FugueDataFrameOperationError("found nonexistent columns: {missing}")
