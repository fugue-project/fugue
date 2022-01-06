import pickle
from threading import RLock
from typing import Any, List, Optional

import dask.dataframe as dd
import pandas as pd
from fugue import transform
from fugue.collections.partition import PartitionSpec
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.workflow.workflow import FugueWorkflow
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests

from fugue_dask.execution_engine import DaskExecutionEngine


class DaskExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = DaskExecutionEngine(dict(test=True))
        return e

    def test__join_outer_pandas_incompatible(self):
        return

    def test_map_with_dict_col(self):
        # TODO: add back
        return

    def test_to_df(self):
        e = self.engine
        a = e.to_df([[1, 2], [3, 4]], "a:int,b:int", dict(a=1))
        df_eq(a, [[1, 2], [3, 4]], "a:int,b:int", dict(a=1), throw=True)
        a = e.to_df(PandasDataFrame([[1, 2], [3, 4]], "a:int,b:int", dict(a=1)))
        df_eq(a, [[1, 2], [3, 4]], "a:int,b:int", dict(a=1), throw=True)
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

    def test_sample_n(self):
        # TODO: dask does not support sample by number of rows
        pass


class DaskExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = DaskExecutionEngine(dict(test=True))
        return e

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

        with self.dag() as dag:
            df = dag.create(m_c).process(m_p)
            df.assert_eq(dag.df([[0]], "a:long"))
            df.output(m_o)


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
        force_output_fugue_dataframe=True,
        engine="dask",
    )
    assert res.is_local
    assert 5 == res.count()
    assert 5 == cb.n

    res = transform(
        pdf,
        tr,
        schema="b:binary",
        force_output_fugue_dataframe=True,
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
        force_output_fugue_dataframe=True,
        engine="dask",
        persist=True,  # when you have a persist, you can use callback
    )
    assert not res.is_local
    assert 5 == res.count()
    assert 5 == cb.n
