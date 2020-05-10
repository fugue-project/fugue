from fugue_modin.execution_engine import ModinExecutionEngine
from fugue_test.execution_suite import ExecutionEngineTests
from fugue_test.builtin_suite import BuiltInTests


class ModinExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = ModinExecutionEngine()
        return e

    def test__join_outer_pandas_incompatible(self):
        return


class ModinExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = ModinExecutionEngine()
        return e
