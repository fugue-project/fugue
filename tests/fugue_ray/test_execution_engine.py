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

    def test_repartitioning(self):
        # schema: *
        def t(df: pd.DataFrame) -> pd.DataFrame:
            return df.head(1)

        test = pd.DataFrame(dict(a=[1, 2, 3]))

        # even partitioning
        res = transform(
            test,
            t,
            partition="per_row",
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

        # rand partitioning
        res = transform(
            test,
            t,
            partition=dict(num=3, algo="rand"),
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

        # over partitioning
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

    def test_partition_by_more(self):
        # schema: *
        def t(df: pd.DataFrame) -> pd.DataFrame:
            return df.head(1)

        test = pd.DataFrame(dict(a=[1, 2, 3]))

        res = transform(
            test,
            t,
            partition={"by": "a"},
            engine="ray",
            engine_conf={
                "fugue.ray.shuffle.partitions": 2,
                "fugue.ray.remote.num_cpus": 1,
            },
            as_local=True,
            force_output_fugue_dataframe=True,
        )
        df_eq(
            res,
            ArrayDataFrame([[1], [2], [3]], schema="a:long"),
            check_order=False,
            throw=True,
        )

    def test_remote_args(self):
        e = RayExecutionEngine(conf={"fugue.ray.remote.num_cpus": 3})
        assert e._get_remote_args() == {"num_cpus": 3, "scheduling_strategy": "SPREAD"}

        e = RayExecutionEngine(conf={"fugue.ray.remote.scheduling_strategy": "DEFAULT"})
        assert e._get_remote_args() == {"scheduling_strategy": "DEFAULT"}


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
