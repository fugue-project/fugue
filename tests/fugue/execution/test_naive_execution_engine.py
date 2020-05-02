from fugue.execution import NaiveExecutionEngine
from fugue_test.execution_suite import ExecutionEngineTests


class NaiveExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = NaiveExecutionEngine()
        # e.conf()["fugue.path.temp.cache"] = e.cache().allocate_filename("")
        return e
