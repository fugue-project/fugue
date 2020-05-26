from typing import Any, Dict, Iterable, List, Optional, Tuple

import dask.dataframe as pd
import pandas
from fugue.dataframe import DataFrame, LocalDataFrame, PandasDataFrame
from fugue.dataframe.dataframe import _input_schema
from triad.collections.schema import Schema
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from fugue_dask.utils import DASK_UTILS
from fugue_dask.constants import DEFAULT_CONFIG


class DaskDataFrame(DataFrame):
    def __init__(  # noqa: C901
        self,
        df: Any = None,
        schema: Any = None,
        metadata: Any = None,
        num_partitions: int = 0,
        type_safe=True,
    ):
        if num_partitions <= 0:
            num_partitions = DEFAULT_CONFIG.get_or_throw(
                "fugue.dask.dataframe.default.partitions", int
            )
        if df is None:
            schema = _input_schema(schema).assert_not_empty()
            df = []
        if isinstance(df, DaskDataFrame):
            super().__init__(df.schema, df.metadata if metadata is None else metadata)
            self._native: pd.DataFrame = df._native
            return
        elif isinstance(df, (pd.DataFrame, pd.Series)):
            if isinstance(df, pd.Series):
                df = df.to_frame()
            pdf = df
            schema = None if schema is None else _input_schema(schema)
        elif isinstance(df, (pandas.DataFrame, pandas.Series)):
            if isinstance(df, pandas.Series):
                df = df.to_frame()
            pdf = pd.from_pandas(df, npartitions=num_partitions)
            schema = None if schema is None else _input_schema(schema)
        elif isinstance(df, Iterable):
            assert_arg_not_none(schema, msg="schema can't be None for iterable input")
            schema = _input_schema(schema).assert_not_empty()
            t = PandasDataFrame(df, schema)
            pdf = pd.from_pandas(t.native, npartitions=num_partitions)
            type_safe = False
        else:
            raise ValueError(f"{df} is incompatible with DaskDataFrame")
        pdf, schema = self._apply_schema(pdf, schema, type_safe)
        super().__init__(schema, metadata)
        self._native = pdf

    @property
    def native(self) -> pd.DataFrame:
        return self._native

    @property
    def is_local(self) -> bool:
        return False

    def as_local(self) -> LocalDataFrame:
        return PandasDataFrame(self.as_pandas(), self.schema)

    @property
    def is_bounded(self) -> bool:
        return True

    @property
    def empty(self) -> bool:
        return DASK_UTILS.empty(self.native)

    @property
    def num_partitions(self) -> int:
        return self.native.npartitions

    def peek_array(self) -> Any:
        return self.as_pandas().iloc[0].values.tolist()

    def persist(self, **kwargs: Any) -> "DaskDataFrame":
        self._native = self.native.persist(**kwargs)
        return self

    def count(self) -> int:
        return self.as_pandas().shape[0]

    def as_pandas(self) -> pandas.DataFrame:
        return self.native.compute().reset_index(drop=True)

    def drop(self, cols: List[str]) -> DataFrame:
        try:
            schema = self.schema - cols
        except Exception as e:
            raise InvalidOperationError(str(e))
        if len(schema) == 0:
            raise InvalidOperationError("Can't remove all columns of a dataframe")
        return DaskDataFrame(self.native.drop(cols, axis=1), schema, type_safe=False)

    def rename(self, columns: Dict[str, str]) -> "DataFrame":
        df = self.native.rename(columns=columns)
        schema = self.schema.rename(columns)
        return DaskDataFrame(df, schema, type_safe=False)

    def as_array(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> List[Any]:
        return list(self.as_array_iterable(columns, type_safe=type_safe))

    def as_array_iterable(
        self, columns: Optional[List[str]] = None, type_safe: bool = False
    ) -> Iterable[Any]:
        sub = None if columns is None else self.schema.extract(columns).pa_schema
        return DASK_UTILS.as_array_iterable(
            self.native.compute(), sub, type_safe=type_safe, null_safe=True
        )

    def _apply_schema(
        self, pdf: pd.DataFrame, schema: Optional[Schema], type_safe: bool = True
    ) -> Tuple[pd.DataFrame, Schema]:
        if not type_safe:
            assert_arg_not_none(pdf, "pdf")
            assert_arg_not_none(schema, "schema")
            return pdf, schema
        DASK_UTILS.ensure_compatible(pdf)
        if pdf.columns.dtype == "object":  # pdf has named schema
            pschema = Schema(DASK_UTILS.to_schema(pdf))
            if schema is None or pschema == schema:
                return pdf, pschema.assert_not_empty()
            pdf = pdf[schema.assert_not_empty().names]
        else:  # pdf has no named schema
            schema = _input_schema(schema).assert_not_empty()
            assert_or_throw(
                pdf.shape[1] == len(schema),
                ValueError(f"Pandas datafame column count doesn't match {schema}"),
            )
            pdf.columns = schema.names
        return DASK_UTILS.enforce_type(pdf, schema.pa_schema, null_safe=True), schema
