from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests

from fugue_duckdb import DuckExeuctionEngine


class DuckExecutionEngineSqliteTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = DuckExeuctionEngine(dict(test=True))
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class DuckExecutionEngineBuiltInQPDTests(BuiltInTests.Tests):
    def make_engine(self):
        e = DuckExeuctionEngine(dict(test=True))
        return e
