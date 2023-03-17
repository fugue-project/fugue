import pandas as pd
import pyarrow as pa

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
