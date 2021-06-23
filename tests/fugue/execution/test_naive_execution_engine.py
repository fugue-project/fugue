import inspect
from typing import Any, Optional

import pandas as pd
import pyarrow as pa
from fugue import (
    ArrowDataFrame,
    DataFrame,
    NativeExecutionEngine,
    QPDPandasEngine,
    SqliteEngine,
)
from fugue._utils.interfaceless import (
    DataFrameParam,
    SimpleAnnotationConverter,
    register_annotation_converter,
)
from fugue.execution.execution_engine import ExecutionEngine, _get_file_threshold
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests


class ArrowDataFrameParam(DataFrameParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, annotation="pa.DataFrame")

    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        assert isinstance(ctx, ExecutionEngine)
        return ArrowDataFrame(ctx.to_df(df).as_pandas()).native

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, pa.Table)
        assert isinstance(ctx, ExecutionEngine)
        return ctx.to_df(output.to_pandas(), schema=schema)

    def count(self, df: DataFrame) -> int:
        raise NotImplementedError("not allowed")


register_annotation_converter(
    0.8, SimpleAnnotationConverter(pa.Table, lambda param: ArrowDataFrameParam(param))
)


class NativeExecutionEngineSqliteTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(SqliteEngine(e))
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class NativeExecutionEngineBuiltInSqliteTests(BuiltInTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(SqliteEngine(e))
        return e

    def test_annotation(self):
        def m_c(engine: NativeExecutionEngine) -> pa.Table:
            return pa.Table.from_pandas(pd.DataFrame([[0]], columns=["a"]))

        def m_p(engine: NativeExecutionEngine, df: pa.Table) -> pa.Table:
            return df

        def m_o(engine: NativeExecutionEngine, df: pa.Table) -> None:
            assert 1 == df.to_pandas().shape[0]

        with self.dag() as dag:
            df = dag.create(m_c).process(m_p)
            df.assert_eq(dag.df([[0]], "a:long"))
            df.output(m_o)


class NativeExecutionEngineQPDTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(QPDPandasEngine(e))
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class NativeExecutionEngineBuiltInQPDTests(BuiltInTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(QPDPandasEngine(e))
        return e


def test_get_file_threshold():
    assert -1 == _get_file_threshold(None)
    assert -2 == _get_file_threshold(-2)
    assert 1024 == _get_file_threshold("1k")
