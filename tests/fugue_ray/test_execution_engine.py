import pickle

import duckdb
import pandas as pd
import ray
from fugue import DataFrame, transform, ArrayDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_dask import DaskDataFrame
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_sql import fsql
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from pytest import raises
from fugue.dataframe.utils import _df_eq as df_eq

from fugue_ray import RayExecutionEngine

_CONF = {
    "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
    "fugue.rpc.flask_server.host": "127.0.0.1",
    "fugue.rpc.flask_server.port": "1234",
    "fugue.rpc.flask_server.timeout": "2 sec",
}


class RayExecutionEngineTests(ExecutionEngineTests.Tests):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2)
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        cls._con.close()
        ray.shutdown()

    def make_engine(self):
        e = RayExecutionEngine(
            conf={"test": True, "fugue.duckdb.pragma.threads": 2},
            connection=self._con,
        )
        return e

    def test_over_repartitioning(self):
        # schema: *
        def t(df: pd.DataFrame) -> pd.DataFrame:
            return df.head(1)

        test = pd.DataFrame(dict(a=[1, 2, 3]))

        res = transform(
            test,
            t,
            partition=dict(num=40),
            engine="ray",
            as_local=True,
            force_output_fugue_dataframe=True,
        )
        df_eq(
            res,
            ArrayDataFrame([[1], [2], [3]], schema="a:long"),
            check_order=False,
            throw=True,
        )


class RayBuiltInTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        ray.init(num_cpus=2)
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        cls._con.close()
        ray.shutdown()

    def make_engine(self):
        e = RayExecutionEngine(
            conf={"test": True, "fugue.duckdb.pragma.threads": 2, **_CONF},
            connection=self._con,
        )
        return e

    def _test_special_types(self):
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
