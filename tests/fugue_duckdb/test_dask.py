import pickle

import duckdb
import pandas as pd
import pyarrow as pa
from fugue import ArrowDataFrame, DataFrame, FugueWorkflow
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_sql import fsql
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from pytest import raises

from fugue_duckdb import DuckDaskExecutionEngine
from fugue_duckdb.dataframe import DuckDataFrame


class DuckDaskExecutionEngineTests(ExecutionEngineTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        cls._con.close()

    def make_engine(self):
        e = DuckDaskExecutionEngine(
            {"test": True, "fugue.duckdb.pragma.threads": 2}, self._con
        )
        return e

    def test_intersect_all(self):
        e = self.engine
        a = e.to_df([[1, 2, 3], [4, None, 6], [4, None, 6]], "a:double,b:double,c:int")
        b = e.to_df(
            [[1, 2, 33], [4, None, 6], [4, None, 6], [4, None, 6]],
            "a:double,b:double,c:int",
        )
        c = e.intersect(a, b, distinct=False)
        df_eq(
            c,
            [[4, None, 6], [4, None, 6]],
            "a:double,b:double,c:int",
            throw=True,
        )


class DuckDaskBuiltInTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        cls._con.close()

    def make_engine(self):
        e = DuckDaskExecutionEngine(
            {"test": True, "fugue.duckdb.pragma.threads": 2}, self._con
        )
        return e

    def test_special_types(self):
        def assert_data(df: DataFrame) -> None:
            assert df.schema == "a:datetime,b:bytes,c:[long]"

        df = pd.DataFrame(
            [["2020-01-01", pickle.dumps([1, 2]), [1, 2]]], columns=list("abc")
        )
        df["a"] = pd.to_datetime(df.a)

        with self.dag() as dag:
            x = dag.df(df)
            result = dag.select("SELECT * FROM ", x)
            result.output(assert_data)
