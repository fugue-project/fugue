import pickle
from threading import RLock
from typing import Any, List, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd
from dask.distributed import Client

import fugue.api as fa
import fugue.test as ft
from fugue import (
    ArrayDataFrame,
    FugueWorkflow,
    PandasDataFrame,
    PartitionSpec,
    Transformer,
    transform,
    transformer,
)
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.plugins import infer_execution_engine
from fugue_dask.dataframe import DaskDataFrame
from fugue_dask.execution_engine import DaskExecutionEngine
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from fugue.column import col, all_cols
import fugue.column.functions as ff

_CONF = {
    "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
    "fugue.rpc.flask_server.host": "127.0.0.1",
    "fugue.rpc.flask_server.port": "1234",
    "fugue.rpc.flask_server.timeout": "2 sec",
}


@ft.fugue_test_suite(("dask", _CONF), mark_test=True)
class DaskExecutionEngineTests(ExecutionEngineTests.Tests):
    @property
    def dask_client(self) -> Client:
        return self.context.session

    def test_properties(self):
        assert self.engine.is_distributed
        assert self.engine.map_engine.is_distributed
        assert self.engine.sql_engine.is_distributed

    def test_get_parallelism(self):
        assert fa.get_current_parallelism() == 3

    def test__join_outer_pandas_incompatible(self):
        return

    # TODO: dask-sql 2024.5.0 has a bug, can't pass the HAVING tests
    def test_select(self):
        try:
            import qpd
            import dask_sql
        except ImportError:
            return

        a = ArrayDataFrame(
            [[1, 2], [None, 2], [None, 1], [3, 4], [None, 4]], "a:double,b:int"
        )

        # simple
        b = fa.select(a, col("b"), (col("b") + 1).alias("c").cast(str))
        self.df_eq(
            b,
            [[2, "3"], [2, "3"], [1, "2"], [4, "5"], [4, "5"]],
            "b:int,c:str",
            throw=True,
        )

        # with distinct
        b = fa.select(
            a, col("b"), (col("b") + 1).alias("c").cast(str), distinct=True
        )
        self.df_eq(
            b,
            [[2, "3"], [1, "2"], [4, "5"]],
            "b:int,c:str",
            throw=True,
        )

        # wildcard
        b = fa.select(a, all_cols(), where=col("a") + col("b") == 3)
        self.df_eq(b, [[1, 2]], "a:double,b:int", throw=True)

        # aggregation
        b = fa.select(a, col("a"), ff.sum(col("b")).cast(float).alias("b"))
        self.df_eq(b, [[1, 2], [3, 4], [None, 7]], "a:double,b:double", throw=True)

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


@ft.fugue_test_suite(("dask", _CONF), mark_test=True)
class DaskExecutionEngineBuiltInTests(BuiltInTests.Tests):
    @property
    def dask_client(self) -> Client:
        return self.context.session

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

    def test_repartition(self):
        with FugueWorkflow() as dag:
            a = dag.df(
                [[0, 1], [0, 2], [0, 3], [0, 4], [1, 1], [1, 2], [1, 3]], "a:int,b:int"
            )
            c = a.per_row().transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[1, 1, 1, 1, 1, 1, 1]))
            c = a.partition(algo="even", num="ROWCOUNT/2").transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[3, 3, 1]))
            c = a.per_partition_by("a").transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[3, 4]))
            c = a.partition_by("a", num=3).transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[3, 4]))
            c = a.partition_by("a", num=100, algo="even").transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[3, 4]))
            # c = a.partition(algo="even", by=["a"]).transform(AssertMaxNTransform)
            # dag.output(c, using=assert_match, params=dict(values=[3, 4]))
            c = a.partition(num=1).transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[7]))
            c = a.partition(algo="rand", num=100).transform(count_partition).persist()
            c = a.partition(algo="hash", num=100).transform(count_partition).persist()
        dag.run(self.engine)

    def test_repartition_large(self):
        with FugueWorkflow() as dag:
            a = dag.df([[p, 0] for p in range(100)], "a:int,b:int")
            c = (
                a.partition(algo="even", by=["a"])
                .transform(AssertMaxNTransform)
                .persist()
            )
            c = (
                a.partition(algo="even", num="ROWCOUNT/2", by=["a"])
                .transform(AssertMaxNTransform, params=dict(n=2))
                .persist()
            )
            dag.output(c, using=assert_all_n, params=dict(n=1, l=100))
            c = (
                a.partition(algo="even", num="ROWCOUNT")
                .transform(AssertMaxNTransform)
                .persist()
            )
            dag.output(c, using=assert_all_n, params=dict(n=1, l=100))
            c = a.partition(algo="even", num="ROWCOUNT/2").transform(
                AssertMaxNTransform, params=dict(n=2)
            )
            dag.output(c, using=assert_all_n, params=dict(n=2, l=50))
            c = a.partition(num=1).transform(count_partition)
            dag.output(c, using=assert_match, params=dict(values=[100]))
        dag.run(self.engine)

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


@ft.with_backend("dask")
def test_join_keys_unification(backend_context):
    df1 = DaskDataFrame(
        pd.DataFrame([[10, 1], [11, 3]], columns=["a", "b"]).convert_dtypes(),
        "a:long,b:long",
    )
    df2 = PandasDataFrame(
        pd.DataFrame([[10, [2]]], columns=["a", "c"]),
        "a:long,c:[long]",
    )
    with fa.engine_context(backend_context.session) as engine:
        assert fa.as_array(fa.inner_join(df1, df2)) == [[10, 1, [2]]]
        assert fa.as_array(fa.inner_join(df2, df1)) == [[10, [2], 1]]


@ft.with_backend("dask")
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


@ft.with_backend("dask")
def test_multiple_transforms(backend_context):
    def t1(df: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([df, df])

    def t2(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["a", "b"], as_index=False, dropna=False)
            .apply(lambda x: x.head(1))
            .reset_index(drop=True)
        )

    def compute(df: pd.DataFrame, engine) -> pd.DataFrame:
        with fa.engine_context(engine):
            ddf = fa.as_fugue_df(df)
            ddf1 = fa.transform(ddf, t1, schema="*", partition=dict(algo="hash"))
            ddf2 = fa.transform(
                ddf1,
                t2,
                schema="*",
                partition=dict(by=["a", "b"], presort="c", algo="coarse", num=2),
            )
            return (
                ddf2.as_pandas()
                .astype("float64")
                .fillna(float("nan"))
                .sort_values(["a", "b"])
            )

    np.random.seed(0)
    df = pd.DataFrame(
        dict(
            a=np.random.randint(1, 5, 1000),
            b=np.random.choice([1, 2, 3, None], 1000),
            c=np.random.rand(1000),
        )
    )

    actual = compute(df, backend_context.session)
    expected = compute(df, None)
    assert np.allclose(actual, expected, equal_nan=True)


@transformer("ct:long")
def count_partition(df: List[List[Any]]) -> List[List[Any]]:
    return [[len(df)]]


class AssertMaxNTransform(Transformer):
    def get_output_schema(self, df):
        return "c:int"

    def transform(self, df):
        # need to collect because df can be IterableDataFrame
        # or IterablePandasDataFrame
        pdf = df.as_pandas()
        if not hasattr(self, "called"):
            self.called = 1
            self.data = [str(pdf)]
        else:
            self.called += 1
            self.data.append(str(pdf))
        n = self.params.get("n", 1)
        # if self.called > n:
        #    raise AssertionError(f"{self.data}")
        assert len(pdf) <= n and len(pdf) > 0, f"{self.data}"
        return ArrayDataFrame([[len(pdf)]], "c:int")


def assert_match(df: List[List[Any]], values: List[int]) -> None:
    assert set(values) == set(x[0] for x in df)


def assert_all_n(df: List[List[Any]], n, l) -> None:
    assert all(x[0] == n for x in df), str([x[0] for x in df])
    assert l == len(df)
