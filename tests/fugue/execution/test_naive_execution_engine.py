from fugue.execution import NaiveExecutionEngine
from fugue_test.execution_suite import ExecutionEngineTests
from fugue_test.builtin_suite import BuiltInTests


class NaiveExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = NaiveExecutionEngine(dict(test=True))
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class NaiveExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = NaiveExecutionEngine(dict(test=True))
        return e
