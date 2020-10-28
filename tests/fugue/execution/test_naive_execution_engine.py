import os

from fugue.execution import NativeExecutionEngine
from fugue.execution.execution_engine import _get_file_threshold
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests


class NativeExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class NativeExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = NativeExecutionEngine(dict(test=True))
        return e


def test_get_file_threshold():
    assert -1 == _get_file_threshold(None)
    assert -2 == _get_file_threshold(-2)
    assert 1024 == _get_file_threshold("1k")
