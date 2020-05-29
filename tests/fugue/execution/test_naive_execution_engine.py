from fugue.execution import NativeExecutionEngine
from fugue_test.execution_suite import ExecutionEngineTests
from fugue_test.builtin_suite import BuiltInTests


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
