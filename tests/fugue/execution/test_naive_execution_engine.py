import os

from fugue import NativeExecutionEngine, QPDPandasEngine, SqliteEngine
from fugue.execution.execution_engine import _get_file_threshold
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests


class NativeExecutionEngineSqliteTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(SqliteEngine(e))
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class NativeExecutionEngineBuiltInSqliteTests(BuiltInTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(SqliteEngine(e))
        return e


class NativeExecutionEngineQPDTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(QPDPandasEngine(e))
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class NativeExecutionEngineBuiltInQPDTests(BuiltInTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        e.set_sql_engine(QPDPandasEngine(e))
        return e


def test_get_file_threshold():
    assert -1 == _get_file_threshold(None)
    assert -2 == _get_file_threshold(-2)
    assert 1024 == _get_file_threshold("1k")
