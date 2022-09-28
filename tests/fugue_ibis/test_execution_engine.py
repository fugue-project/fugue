import sys

import pytest
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests

from .mock.execution_engine import MockDuckExecutionEngine


@pytest.mark.skipif(sys.version_info < (3, 8), reason="< 3.8")
class IbisExecutionEngineTests(ExecutionEngineTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        # cls._con.close()
        pass

    def make_engine(self):
        return MockDuckExecutionEngine({"test": True})

    def test_select(self):
        # it can't work properly with DuckDB (hugeint is not recognized)
        pass


@pytest.mark.skipif(sys.version_info < (3, 8), reason="< 3.8")
class DuckBuiltInTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        # cls._con.close()
        pass

    def make_engine(self):
        return MockDuckExecutionEngine({"test": True})

    def test_df_select(self):
        # it can't work properly with DuckDB (hugeint is not recognized)
        pass
