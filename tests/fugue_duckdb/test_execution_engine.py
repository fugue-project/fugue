import pickle

import pandas as pd
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from fugue import DataFrame
from fugue_duckdb import DuckExeuctionEngine


class DuckExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = DuckExeuctionEngine(dict(test=True))
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class DuckBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = DuckExeuctionEngine(dict(test=True))
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
