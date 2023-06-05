import pickle

import dask.dataframe as dd
import duckdb
import pandas as pd
import pyarrow as pa
from dask.distributed import Client
from pytest import raises

import fugue.api as fa
from fugue import ArrowDataFrame, DataFrame, FugueWorkflow, PartitionSpec, fsql
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_dask import DaskDataFrame
from fugue_duckdb import DuckDaskExecutionEngine
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests

_CONF = {
    "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
    "fugue.rpc.flask_server.host": "127.0.0.1",
    "fugue.rpc.flask_server.port": "1234",
    "fugue.rpc.flask_server.timeout": "2 sec",
}


class DuckDaskExecutionEngineTests(ExecutionEngineTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)
        fa.set_global_engine(cls._engine)

    @classmethod
    def tearDownClass(cls):
        fa.clear_global_engine()
        cls._con.close()
        cls._engine.dask_client.close()

    def make_engine(self):
        client = Client(processes=True, n_workers=2, threads_per_worker=1)
        e = DuckDaskExecutionEngine(
            conf={"test": True, "fugue.duckdb.pragma.threads": 2},
            connection=self._con,
            dask_client=client,
        )
        return e
    
    def test_properties(self):
        assert not self.engine.is_distributed
        assert self.engine.map_engine.is_distributed
        assert not self.engine.sql_engine.is_distributed

    def test_get_parallelism(self):
        assert fa.get_current_parallelism() == 2

    def test_to_df_dask(self):
        pdf = pd.DataFrame([[1.1]], columns=["a"])
        df = dd.from_pandas(pdf, npartitions=2)
        assert isinstance(self.engine.to_df(df), DuckDataFrame)
        assert isinstance(self.engine._to_auto_df(pdf), DuckDataFrame)
        assert isinstance(self.engine._to_auto_df(df), DaskDataFrame)

        df = dd.from_pandas(pd.DataFrame([[{"a": "b"}]], columns=["a"]), npartitions=2)
        xdf = self.engine.to_df(df, schema="a:{a:str}")
        assert isinstance(xdf, DuckDataFrame)
        assert xdf.schema == "a:{a:str}"

        ddf = DaskDataFrame(df)
        assert isinstance(self.engine.to_df(ddf), DuckDataFrame)
        assert isinstance(self.engine._to_auto_df(ddf), DaskDataFrame)

    def test_repartition_dask(self):
        pdf = pd.DataFrame([[1.1]], columns=["a"])
        assert isinstance(
            self.engine.repartition(self.engine.to_df(pdf), PartitionSpec(num=1)),
            DuckDataFrame,
        )

        df = dd.from_pandas(pdf, npartitions=2)
        ddf = DaskDataFrame(df)
        assert isinstance(
            self.engine.repartition(ddf, PartitionSpec(num=1)), DaskDataFrame
        )

    def test_broadcast(self):
        pdf = pd.DataFrame([[1.1]], columns=["a"])
        assert isinstance(self.engine.broadcast(self.engine.to_df(pdf)), DuckDataFrame)

        df = dd.from_pandas(pdf, npartitions=2)
        ddf = DaskDataFrame(df)
        assert isinstance(self.engine.broadcast(ddf), DaskDataFrame)


class DuckDaskBuiltInTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = duckdb.connect()
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        cls._con.close()
        cls._engine.dask_client.close()

    def make_engine(self):
        e = DuckDaskExecutionEngine(
            conf={"test": True, "fugue.duckdb.pragma.threads": 2, **_CONF},
            connection=self._con,
        )
        return e

    def test_special_types(self):
        def assert_data(df: DataFrame) -> None:
            assert df.schema == "a:datetime,b:bytes,c:[long]"

        df = pd.DataFrame(
            [["2020-01-01", pickle.dumps([1, 2]), [1, 2]]], columns=list("abc")
        )
        df["a"] = pd.to_datetime(df.a)

        with FugueWorkflow() as dag:
            x = dag.df(df)
            result = dag.select("SELECT * FROM ", x)
            result.output(assert_data)
        dag.run(self.engine)

    def test_yield_2(self):
        def assert_data(df: DataFrame) -> None:
            assert df.schema == "a:datetime,b:bytes,c:[long]"

        df = pd.DataFrame([[1, 2, 3]], columns=list("abc"))

        with FugueWorkflow() as dag:
            x = dag.df(df)
            result = dag.select("SELECT * FROM ", x)
            result.yield_dataframe_as("x")
        res = dag.run(self.engine)
        assert res["x"].as_array() == [[1, 2, 3]]
