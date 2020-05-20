from fugue.execution import NaiveExecutionEngine
from fugue_test.execution_suite import ExecutionEngineTests
from fugue_test.builtin_suite import BuiltInTests


class NaiveExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = NaiveExecutionEngine(dict(test=True))
        return e


class NaiveExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = NaiveExecutionEngine(dict(test=True))
        return e
