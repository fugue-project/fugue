import os
import pickle

import duckdb
import pandas as pd
import ray
from fugue import ArrayDataFrame, DataFrame, transform
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_dask import DaskDataFrame
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_sql import fsql
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from pytest import raises
from triad import FileSystem

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

    def test_load_parquet_more(self):
        pq = os.path.join(str(self.tmpdir), "tmp.parquet")
        pd.DataFrame(dict(a=[1, 2], b=[3, 4], c=["a", "b"])).to_parquet(pq)
        tdf = self.engine.load_df(pq, columns="b:int,a:str").as_array()
        assert tdf == [[3, "1"], [4, "2"]]

    def test_load_json_more(self):
        js = os.path.join(str(self.tmpdir), "tmp.json")
        pdf = pd.DataFrame(dict(a=[1, 2], b=[3, 4], c=["a", "b"]))
        self.engine.save_df(self.engine.to_df(pdf), js)
        tdf = self.engine.load_df(js).as_array()
        assert tdf == [[1, 3, "a"], [2, 4, "b"]]
        tdf = self.engine.load_df(js, columns="b:int,a:str").as_array()
        assert tdf == [[3, "1"], [4, "2"]]

    def test_remote_args(self):
        e = RayExecutionEngine(conf={"fugue.ray.remote.num_cpus": 3})
        assert e._get_remote_args() == {"num_cpus": 3}

        e = RayExecutionEngine(conf={"fugue.ray.remote.scheduling_strategy": "SPREAD"})
        assert e._get_remote_args() == {"scheduling_strategy": "SPREAD"}


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

    def test_io(self):
        path = os.path.join(self.tmpdir, "a")
        path2 = os.path.join(self.tmpdir, "b.test.csv")
        path3 = os.path.join(self.tmpdir, "c.partition")
        with self.dag() as dag:
            b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
            b.partition(num=3).save(path, fmt="parquet", single=True)
            b.save(path2, header=True)
        assert FileSystem().isfile(path)
        with self.dag() as dag:
            a = dag.load(path, fmt="parquet", columns=["a", "c"])
            a.assert_eq(dag.df([[1, 6], [7, 2]], "a:long,c:int"))
            a = dag.load(path2, header=True, columns="c:int,a:long")
            a.assert_eq(dag.df([[6, 1], [2, 7]], "c:int,a:long"))

        return
        # TODO: the following (writing partitions) is not supported by Ray
        # with self.dag() as dag:
        #     b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
        #     b.partition(by="c").save(path3, fmt="parquet", single=False)
        # assert FileSystem().isdir(path3)
        # assert FileSystem().isdir(os.path.join(path3, "c=6"))
        # assert FileSystem().isdir(os.path.join(path3, "c=2"))
        # # TODO: in test below, once issue #288 is fixed, use dag.load
        # #  instead of pd.read_parquet
        # pd.testing.assert_frame_equal(
        #     pd.read_parquet(path3).sort_values("a").reset_index(drop=True),
        #     pd.DataFrame({"c": pd.Categorical([6, 2]), "a": [1, 7]}).reset_index(
        #         drop=True
        #     ),
        #     check_like=True,
        # )
