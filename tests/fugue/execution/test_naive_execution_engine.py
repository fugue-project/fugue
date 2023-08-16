from typing import Any, Iterable, List

import pandas as pd
import pyarrow as pa

import fugue.api as fa
from fugue import FugueWorkflow, NativeExecutionEngine, QPDPandasEngine
from fugue.execution.execution_engine import _get_file_threshold
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests


class NativeExecutionEngineQPDTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(QPDPandasEngine(e))
        return e

    def test_properties(self):
        assert not self.engine.is_distributed
        assert not self.engine.map_engine.is_distributed
        assert not self.engine.sql_engine.is_distributed
        assert self.engine.map_engine.conf is self.engine.conf
        assert self.engine.sql_engine.conf is self.engine.conf

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class NativeExecutionEngineBuiltInQPDTests(BuiltInTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(QPDPandasEngine(e))
        return e

    def test_yield_table(self):
        pass

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
                params=dict(rc=0, n=gps, check_ordered=True),
            )
        dag.run(self.engine)

    def test_empty_partition(self):
        def tr(df: pd.DataFrame) -> Iterable[pd.DataFrame]:
            if df.a.iloc[0] == 0:
                yield df

        pdf = pd.DataFrame(dict(a=range(5)))
        res = fa.transform(pdf, tr, schema="*", partition="a")
        assert len(res) == 1
        assert res.values.tolist() == [[0]]
        # assert res.dtypes[0] == pd.Int64Dtype()

        pdf = pd.DataFrame(dict(a=[2, 3]))
        res = fa.transform(pdf, tr, schema="*", partition="a")
        assert len(res) == 0
        # assert res.dtypes[0] == pd.Int64Dtype()

    def test_multiple_partitions(self):
        def assert_one_item(df: pd.DataFrame) -> pd.DataFrame:
            assert 1 == len(df)
            return df

        df = pd.DataFrame(dict(a=[1, 2, 3]))
        res = fa.transform(df, assert_one_item, schema="*", partition="per_row")
        assert res.values.tolist() == [[1], [2], [3]]

        def num_part(df: pd.DataFrame) -> pd.DataFrame:
            return df.assign(b=len(df))

        res = fa.transform(df, num_part, schema="*,b:long", partition=2)
        assert res.values.tolist() == [[1, 2], [2, 2], [3, 1]]


def test_get_file_threshold():
    assert -1 == _get_file_threshold(None)
    assert -2 == _get_file_threshold(-2)
    assert 1024 == _get_file_threshold("1k")


def test_annotations():
    def cr() -> pa.Table:
        return pa.Table.from_pandas(pd.DataFrame([[0]], columns=["a"]))

    def pr(df: pa.Table) -> pd.DataFrame:
        return df.to_pandas()

    def ot(df: pa.Table) -> None:
        assert 1 == df.to_pandas().shape[0]

    dag = FugueWorkflow()
    dag.create(cr).process(pr).output(ot)

    dag.run()
