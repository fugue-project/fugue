import pickle

import pandas as pd
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from fugue import DataFrame
from fugue_duckdb import DuckExecutionEngine
import duckdb


class DuckExecutionEngineTests(ExecutionEngineTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        cls._con.close()

    def make_engine(self):
        e = DuckExecutionEngine(dict(test=True), self._con)
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return

    def test_intersect(self):
        return


class DuckBuiltInTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        cls._con.close()

    def make_engine(self):
        e = DuckExecutionEngine(dict(test=True), self._con)
        return e

    def test_subtract(self):
        return

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
