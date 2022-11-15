import inspect
import logging
import os
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd
from qpd_pandas import run_sql_on_pandas
from qpd_pandas.engine import PandasUtils
from sqlalchemy import create_engine
from triad.collections import Schema
from triad.collections.dict import IndexedOrderedDict
from triad.collections.fs import FileSystem
from triad.utils.assertion import assert_or_throw

from fugue._utils.interfaceless import (
    ExecutionEngineParam,
    SimpleAnnotationConverter,
    register_annotation_converter,
)
from fugue._utils.io import load_df, save_df
from fugue.collections.partition import (
    EMPTY_PARTITION_SPEC,
    PartitionCursor,
    PartitionSpec,
    parse_presort_exp,
)
from fugue.dataframe import (
    DataFrame,
    DataFrames,
    LocalBoundedDataFrame,
    LocalDataFrame,
    PandasDataFrame,
    to_local_bounded_df,
)
from fugue.dataframe.utils import get_join_schemas, to_local_df
from fugue.execution.execution_engine import (
    _DEFAULT_JOIN_KEYS,
    ExecutionEngine,
    MapEngine,
    SQLEngine,
)


class SqliteEngine(SQLEngine):
    """Sqlite execution implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        sql_engine = create_engine("sqlite:///:memory:")
        for k, v in dfs.items():
            v.as_pandas().to_sql(k, sql_engine, if_exists="replace", index=False)
        df = pd.read_sql_query(statement, sql_engine)
        return PandasDataFrame(df)


class QPDPandasEngine(SQLEngine):
    """QPD execution implementation.

    :param execution_engine: the execution engine this sql engine will run on
    """

    def select(self, dfs: DataFrames, statement: str) -> DataFrame:
        _dfs = {
            k: self.execution_engine.to_df(v).as_pandas()  # type: ignore
            for k, v in dfs.items()
        }
        df = run_sql_on_pandas(statement, _dfs, ignore_case=True)
        return self.execution_engine.to_df(df)


class PandasMapEngine(MapEngine):
    def map_dataframe(
        self,
        df: DataFrame,
        map_func: Callable[[PartitionCursor, LocalDataFrame], LocalDataFrame],
        output_schema: Any,
        partition_spec: PartitionSpec,
        on_init: Optional[Callable[[int, DataFrame], Any]] = None,
    ) -> DataFrame:
        if partition_spec.num_partitions != "0":
            self.execution_engine.log.warning(
                "%s doesn't respect num_partitions %s",
                self,
                partition_spec.num_partitions,
            )
        cursor = partition_spec.get_cursor(df.schema, 0)
        if on_init is not None:
            on_init(0, df)
        if len(partition_spec.partition_by) == 0:  # no partition
            df = to_local_df(df)
            cursor.set(df.peek_array(), 0, 0)
            output_df = map_func(cursor, df)
            if (
                isinstance(output_df, PandasDataFrame)
                and output_df.schema != output_schema
            ):
                output_df = PandasDataFrame(output_df.native, output_schema)
            assert_or_throw(
                output_df.schema == output_schema,
                lambda: f"map output {output_df.schema} "
                f"mismatches given {output_schema}",
            )
            return self.execution_engine._to_native_df(output_df)  # type: ignore
        presort = partition_spec.presort
        presort_keys = list(presort.keys())
        presort_asc = list(presort.values())
        output_schema = Schema(output_schema)

        def _map(pdf: pd.DataFrame) -> pd.DataFrame:
            if len(presort_keys) > 0:
                pdf = pdf.sort_values(presort_keys, ascending=presort_asc).reset_index(
                    drop=True
                )
            input_df = PandasDataFrame(pdf, df.schema, pandas_df_wrapper=True)
            cursor.set(input_df.peek_array(), cursor.partition_no + 1, 0)
            output_df = map_func(cursor, input_df)
            return output_df.as_pandas()

        result = self.execution_engine.pl_utils.safe_groupby_apply(  # type: ignore
            df.as_pandas(), partition_spec.partition_by, _map
        )
        return PandasDataFrame(result, output_schema)


class NativeExecutionEngine(ExecutionEngine):
    """The execution engine based on native python and pandas. This execution engine
    is mainly for prototyping and unit tests.

    Please read |ExecutionEngineTutorial| to understand this important Fugue concept

    :param conf: |ParamsLikeObject|, read |FugueConfig| to learn Fugue specific options
    """

    def __init__(self, conf: Any = None):
        super().__init__(conf)
        self._fs = FileSystem()
        self._log = logging.getLogger()

    def __repr__(self) -> str:
        return "NativeExecutionEngine"

    @property
    def log(self) -> logging.Logger:
        return self._log

    @property
    def fs(self) -> FileSystem:
        return self._fs

    def create_default_sql_engine(self) -> SQLEngine:
        return QPDPandasEngine(self)

    def create_default_map_engine(self) -> MapEngine:
        return PandasMapEngine(self)

    @property
    def pl_utils(self) -> PandasUtils:
        """Pandas-like dataframe utils"""
        return PandasUtils()

    def to_df(self, df: Any, schema: Any = None) -> LocalBoundedDataFrame:
        return self._to_native_df(df, schema)

    def _to_native_df(self, df: Any, schema: Any = None) -> LocalBoundedDataFrame:
        return to_local_bounded_df(df, schema)

    def repartition(
        self, df: DataFrame, partition_spec: PartitionSpec
    ) -> DataFrame:  # pragma: no cover
        self.log.warning("%s doesn't respect repartition", self)
        return df

    def broadcast(self, df: DataFrame) -> DataFrame:
        return self._to_native_df(df)

    def persist(
        self,
        df: DataFrame,
        lazy: bool = False,
        **kwargs: Any,
    ) -> DataFrame:
        return self._to_native_df(df)

    def join(
        self,
        df1: DataFrame,
        df2: DataFrame,
        how: str,
        on: List[str] = _DEFAULT_JOIN_KEYS,
    ) -> DataFrame:
        key_schema, output_schema = get_join_schemas(df1, df2, how=how, on=on)
        d = self.pl_utils.join(
            df1.as_pandas(), df2.as_pandas(), join_type=how, on=key_schema.names
        )
        return PandasDataFrame(d.reset_index(drop=True), output_schema)

    def union(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
    ) -> DataFrame:
        assert_or_throw(
            df1.schema == df2.schema,
            lambda: ValueError(f"{df1.schema} != {df2.schema}"),
        )
        d = self.pl_utils.union(df1.as_pandas(), df2.as_pandas(), unique=distinct)
        return PandasDataFrame(d.reset_index(drop=True), df1.schema)

    def subtract(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
    ) -> DataFrame:
        assert_or_throw(
            distinct, NotImplementedError("EXCEPT ALL for NativeExecutionEngine")
        )
        assert_or_throw(
            df1.schema == df2.schema,
            lambda: ValueError(f"{df1.schema} != {df2.schema}"),
        )
        d = self.pl_utils.except_df(df1.as_pandas(), df2.as_pandas(), unique=distinct)
        return PandasDataFrame(d.reset_index(drop=True), df1.schema)

    def intersect(
        self,
        df1: DataFrame,
        df2: DataFrame,
        distinct: bool = True,
    ) -> DataFrame:
        assert_or_throw(
            distinct, NotImplementedError("INTERSECT ALL for NativeExecutionEngine")
        )
        assert_or_throw(
            df1.schema == df2.schema,
            lambda: ValueError(f"{df1.schema} != {df2.schema}"),
        )
        d = self.pl_utils.intersect(df1.as_pandas(), df2.as_pandas(), unique=distinct)
        return PandasDataFrame(d.reset_index(drop=True), df1.schema)

    def distinct(
        self,
        df: DataFrame,
    ) -> DataFrame:
        d = self.pl_utils.drop_duplicates(df.as_pandas())
        return PandasDataFrame(d.reset_index(drop=True), df.schema)

    def dropna(
        self,
        df: DataFrame,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: List[str] = None,
    ) -> DataFrame:
        kwargs: Dict[str, Any] = dict(axis=0, subset=subset, inplace=False)
        if thresh is None:
            kwargs["how"] = how
        else:
            kwargs["thresh"] = thresh
        d = df.as_pandas().dropna(**kwargs)
        return PandasDataFrame(d.reset_index(drop=True), df.schema)

    def fillna(
        self,
        df: DataFrame,
        value: Any,
        subset: List[str] = None,
    ) -> DataFrame:
        assert_or_throw(
            (not isinstance(value, list)) and (value is not None),
            ValueError("fillna value can not None or a list"),
        )
        if isinstance(value, dict):
            assert_or_throw(
                (None not in value.values()) and (any(value.values())),
                ValueError(
                    "fillna dict can not contain None and needs at least one value"
                ),
            )
            mapping = value
        else:
            # If subset is none, apply to all columns
            subset = subset or df.schema.names
            mapping = {col: value for col in subset}
        d = df.as_pandas().fillna(mapping, inplace=False)
        return PandasDataFrame(d.reset_index(drop=True), df.schema)

    def sample(
        self,
        df: DataFrame,
        n: Optional[int] = None,
        frac: Optional[float] = None,
        replace: bool = False,
        seed: Optional[int] = None,
    ) -> DataFrame:
        assert_or_throw(
            (n is None and frac is not None) or (n is not None and frac is None),
            ValueError("one and only one of n and frac should be set"),
        )
        d = df.as_pandas().sample(n=n, frac=frac, replace=replace, random_state=seed)
        return PandasDataFrame(d.reset_index(drop=True), df.schema)

    def take(
        self,
        df: DataFrame,
        n: int,
        presort: str,
        na_position: str = "last",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
    ) -> DataFrame:
        assert_or_throw(
            isinstance(n, int),
            ValueError("n needs to be an integer"),
        )
        d = df.as_pandas()

        # Use presort over partition_spec.presort if possible
        if presort:
            presort = parse_presort_exp(presort)
        _presort: IndexedOrderedDict = presort or partition_spec.presort

        if len(_presort.keys()) > 0:
            d = d.sort_values(
                list(_presort.keys()),
                ascending=list(_presort.values()),
                na_position=na_position,
            )

        if len(partition_spec.partition_by) == 0:
            d = d.head(n)
        else:
            d = d.groupby(by=partition_spec.partition_by, dropna=False).head(n)

        return PandasDataFrame(
            d.reset_index(drop=True), df.schema, pandas_df_wrapper=True
        )

    def load_df(
        self,
        path: Union[str, List[str]],
        format_hint: Any = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> LocalBoundedDataFrame:
        return self._to_native_df(
            load_df(
                path, format_hint=format_hint, columns=columns, fs=self.fs, **kwargs
            )
        )

    def save_df(
        self,
        df: DataFrame,
        path: str,
        format_hint: Any = None,
        mode: str = "overwrite",
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:
        if not force_single and not partition_spec.empty:
            kwargs["partition_cols"] = partition_spec.partition_by
        self.fs.makedirs(os.path.dirname(path), recreate=True)
        df = self._to_native_df(df)
        save_df(df, path, format_hint=format_hint, mode=mode, fs=self.fs, **kwargs)


class _NativeExecutionEngineParam(ExecutionEngineParam):
    def __init__(
        self,
        param: Optional[inspect.Parameter],
    ):
        super().__init__(
            param, annotation="NativeExecutionEngine", engine_type=NativeExecutionEngine
        )


register_annotation_converter(
    0.8,
    SimpleAnnotationConverter(
        NativeExecutionEngine,
        lambda param: _NativeExecutionEngineParam(param),
    ),
)
