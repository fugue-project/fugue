import os
from typing import Any, List

import pandas as pd
import ray.data as rd
from triad.utils.io import isfile

import fugue.api as fa
import fugue.test as ft
from fugue import ArrayDataFrame, DataFrame, FugueWorkflow, transform
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.plugins import infer_execution_engine
from fugue_ray import RayDataFrame, RayExecutionEngine
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests

_CONF = {
    "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
    "fugue.rpc.flask_server.host": "127.0.0.1",
    "fugue.rpc.flask_server.port": "1234",
    "fugue.rpc.flask_server.timeout": "2 sec",
}


@ft.fugue_test_suite(("ray", _CONF), mark_test=True)
class RayExecutionEngineTests(ExecutionEngineTests.Tests):
    def test_properties(self):
        assert self.engine.is_distributed
        assert self.engine.map_engine.is_distributed
        assert not self.engine.sql_engine.is_distributed

    def test_get_parallelism(self):
        assert fa.get_current_parallelism() == 2

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
            as_fugue=True,
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
            as_fugue=True,
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
            as_fugue=True,
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
            as_fugue=True,
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
        assert sorted(tdf) == [[3, "1"], [4, "2"]]

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

    def test_infer_engine(self):
        df = rd.from_pandas(pd.DataFrame([[0]], columns=["a"]))
        assert infer_execution_engine([df]) == "ray"

        fdf = RayDataFrame(df)
        assert infer_execution_engine([fdf]) == "ray"


@ft.fugue_test_suite(("ray", _CONF), mark_test=True)
class RayBuiltInTests(BuiltInTests.Tests):
    def test_datetime_in_workflow(self):
        pass

    def test_yield_table(self):
        pass

    def test_yield_2(self):
        try:
            import sqlglot
        except ImportError:
            return

        def assert_data(df: DataFrame) -> None:
            assert df.schema == "a:datetime,b:bytes,c:[long]"

        df = pd.DataFrame([[1, 2, 3]], columns=list("abc"))

        with FugueWorkflow() as dag:
            x = dag.df(df)
            result = dag.select("SELECT * FROM ", x)
            result.yield_dataframe_as("x")
        res = dag.run(self.engine)
        assert res["x"].as_array() == [[1, 2, 3]]

    def test_io(self):
        path = os.path.join(self.tmpdir, "a")
        path2 = os.path.join(self.tmpdir, "b.test.csv")
        path3 = os.path.join(self.tmpdir, "c.partition")
        with FugueWorkflow() as dag:
            b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
            b.partition(num=3).save(path, fmt="parquet", single=True)
            b.save(path2, header=True)
        dag.run(self.engine)
        assert isfile(path)
        with FugueWorkflow() as dag:
            a = dag.load(path, fmt="parquet", columns=["a", "c"])
            a.assert_eq(dag.df([[1, 6], [7, 2]], "a:long,c:int"))
            a = dag.load(path2, header=True, columns="c:int,a:long")
            a.assert_eq(dag.df([[6, 1], [2, 7]], "c:int,a:long"))
        dag.run(self.engine)

        return
        # TODO: the following (writing partitions) is not supported by Ray
        # with FugueWorkflow() as dag:
        #     b = dag.df([[6, 1], [2, 7]], "c:int,a:long")
        #     b.partition(by="c").save(path3, fmt="parquet", single=False)
        # assert isdir(path3)
        # assert isdir(os.path.join(path3, "c=6"))
        # assert isdir(os.path.join(path3, "c=2"))
        # # TODO: in test below, once issue #288 is fixed, use dag.load
        # #  instead of pd.read_parquet
        # pd.testing.assert_frame_equal(
        #     pd.read_parquet(path3).sort_values("a").reset_index(drop=True),
        #     pd.DataFrame({"c": pd.Categorical([6, 2]), "a": [1, 7]}).reset_index(
        #         drop=True
        #     ),
        #     check_like=True,
        # )

    def test_coarse_partition(self):
        def verify_coarse_partition(df: pd.DataFrame) -> List[List[Any]]:
            ct = df.a.nunique()
            s = df.a * 1000 + df.b
            ordered = ((s - s.shift(1)).dropna() >= 0).all(axis=None)
            return [[ct, ordered]]

        def assert_(df: pd.DataFrame, rc: int, n: int, check_ordered: bool) -> None:
            if rc > 0:
                assert len(df) == rc
            assert df.ct.sum() == n
            if check_ordered:
                assert (df.ordered == True).all()

        gps = 100
        partition_num = 6
        df = pd.DataFrame(dict(a=list(range(gps)) * 10, b=range(gps * 10))).sample(
            frac=1.0
        )
        with FugueWorkflow() as dag:
            a = dag.df(df)
            c = a.partition(
                algo="coarse", by="a", presort="b", num=partition_num
            ).transform(verify_coarse_partition, schema="ct:int,ordered:bool")
            dag.output(
                c,
                using=assert_,
                params=dict(rc=partition_num, n=gps, check_ordered=True),
            )
        dag.run(self.engine)
