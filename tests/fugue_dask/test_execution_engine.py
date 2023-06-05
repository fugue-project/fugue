import pickle
from threading import RLock
from typing import Any, List, Optional

import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
import dask

import fugue.api as fa
from fugue import transform
from fugue.collections.partition import PartitionSpec
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.plugins import infer_execution_engine
from fugue.workflow.workflow import FugueWorkflow
from fugue_dask.dataframe import DaskDataFrame
from fugue_dask.execution_engine import DaskExecutionEngine
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests

_CONF = {
    "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
    "fugue.rpc.flask_server.host": "127.0.0.1",
    "fugue.rpc.flask_server.port": "1234",
    "fugue.rpc.flask_server.timeout": "2 sec",
}


class DaskExecutionEngineTests(ExecutionEngineTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._engine = cls.make_engine(cls)
        fa.set_global_engine(cls._engine)

    @classmethod
    def tearDownClass(cls):
        fa.clear_global_engine()
        cls._engine.dask_client.close()

    def make_engine(self):
        client = Client(processes=True, n_workers=3, threads_per_worker=1)
        # p2p (new default algo has bugs)
        dask.config.set({"dataframe.shuffle.method": "tasks"})
        e = DaskExecutionEngine(client, conf=dict(test=True, **_CONF))
        return e

    def test_properties(self):
        assert self.engine.is_distributed
        assert self.engine.map_engine.is_distributed
        assert self.engine.sql_engine.is_distributed

    def test_get_parallelism(self):
        assert fa.get_current_parallelism() == 3

    def test__join_outer_pandas_incompatible(self):
        return

    def test_to_df(self):
        e = self.engine
        a = e.to_df([[1, 2], [3, 4]], "a:int,b:int")
        df_eq(a, [[1, 2], [3, 4]], "a:int,b:int", throw=True)
        a = e.to_df(PandasDataFrame([[1, 2], [3, 4]], "a:int,b:int"))
        df_eq(a, [[1, 2], [3, 4]], "a:int,b:int", throw=True)
        assert a is e.to_df(a)

    def test_repartition(self):
        e = self.engine
        a = e.to_df([[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]], "a:int,b:int")
        b = e.repartition(a, PartitionSpec())
        assert a is b
        b = e.repartition(a, PartitionSpec(num=3))
        assert 3 == b.num_partitions
        b = e.repartition(a, PartitionSpec(num="0"))
        assert a is b
        b = e.repartition(a, PartitionSpec(num="ROWCOUNT"))
        assert 5 == b.num_partitions
        b = e.repartition(a, PartitionSpec(num="ROWCOUNT/2"))
        assert 2 == b.num_partitions
        b = e.repartition(a, PartitionSpec(num="ROWCOUNT-ROWCOUNT"))
        assert a is b
        b = e.repartition(a, PartitionSpec(by=["a"], num=3))
        assert a.num_partitions == b.num_partitions

        b = e.repartition(a, PartitionSpec(num=5, algo="even"))
        assert 5 == b.num_partitions
        for i in range(b.num_partitions):
            assert len(b.native.partitions[i]) == 1

        b = e.repartition(a, PartitionSpec(num="ROWCOUNT", algo="even"))
        assert 5 == b.num_partitions
        for i in range(b.num_partitions):
            assert len(b.native.partitions[i]) == 1

        b = e.repartition(a, PartitionSpec(num="ROWCOUNT*2", algo="even"))
        assert 5 == b.num_partitions
        for i in range(b.num_partitions):
            assert len(b.native.partitions[i]) == 1

        b = e.repartition(a, PartitionSpec(num="ROWCOUNT-4", algo="even"))
        assert 1 == b.num_partitions
        for i in range(b.num_partitions):
            assert len(b.native.partitions[i]) == 5

        b = e.repartition(a, PartitionSpec(num="ROWCOUNT-ROWCOUNT", algo="even"))
        assert a is b

    def test_sample_n(self):
        # TODO: dask does not support sample by number of rows
        pass

    def test_infer_engine(self):
        ddf = dd.from_pandas(pd.DataFrame([[0]], columns=["a"]), npartitions=2)
        assert isinstance(infer_execution_engine([ddf]), Client)

        fdf = DaskDataFrame(ddf)
        assert isinstance(infer_execution_engine([fdf]), Client)


class DaskExecutionEngineBuiltInTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._engine = cls.make_engine(cls)
        fa.set_global_engine(cls._engine)

    @classmethod
    def tearDownClass(cls):
        fa.clear_global_engine()
        cls._engine.dask_client.close()

    def make_engine(self):
        e = DaskExecutionEngine(conf=dict(test=True, **_CONF))
        return e

    def test_yield_table(self):
        pass

    def test_default_init(self):
        a = FugueWorkflow().df([[0]], "a:int")
        df_eq(a.compute(DaskExecutionEngine), [[0]], "a:int")

    def test_annotation(self):
        def m_c(engine: DaskExecutionEngine) -> dd.DataFrame:
            return dd.from_pandas(pd.DataFrame([[0]], columns=["a"]), npartitions=2)

        def m_p(engine: DaskExecutionEngine, df: dd.DataFrame) -> dd.DataFrame:
            return df

        def m_o(engine: DaskExecutionEngine, df: dd.DataFrame) -> None:
            assert 1 == df.compute().shape[0]

        with FugueWorkflow() as dag:
            df = dag.create(m_c).process(m_p)
            df.assert_eq(dag.df([[0]], "a:long"))
            df.output(m_o)
        dag.run(self.engine)

    def test_bool_bytes_union(self):
        # this is to verify a bug in enforce type is fixed
        def tr(df: pd.DataFrame) -> pd.DataFrame:
            return df.assign(data=b"asdf")

        df = pd.DataFrame(dict(a=[True, False], b=[1, 2]))

        r1 = fa.transform(df, tr, schema="*,data:bytes", as_fugue=True)
        r2 = fa.transform(df, tr, schema="*,data:bytes", as_fugue=True)
        r3 = fa.union(r1, r2, distinct=False)
        r3.show()

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


def test_transform():
    class CB:
        def __init__(self):
            self._lock = RLock()
            self.n = 0

        def add(self, n):
            with self._lock:
                self.n += n

    cb = CB()

    def tr(df: List[List[Any]], add: Optional[callable]) -> List[List[Any]]:
        if add is not None:
            add(len(df))
        return [[pickle.dumps(x[0])] for x in df]

    pdf = pd.DataFrame(dict(a=list(range(5))))
    res = transform(
        pdf,
        tr,
        schema="b:binary",
        callback=cb.add,
        as_local=True,
        as_fugue=True,
        engine="dask",
        engine_conf=_CONF,
    )
    assert res.is_local
    assert 5 == res.count()
    assert 5 == cb.n

    res = transform(
        pdf,
        tr,
        schema="b:binary",
        as_fugue=True,
        engine="dask",
    )
    assert not res.is_local
    assert 5 == res.count()

    cb = CB()

    res = transform(
        pdf,
        tr,
        schema="b:binary",
        callback=cb.add,
        as_fugue=True,
        engine="dask",
        engine_conf=_CONF,
        persist=True,  # when you have a persist, you can use callback
    )
    assert not res.is_local
    assert 5 == res.count()
    assert 5 == cb.n
