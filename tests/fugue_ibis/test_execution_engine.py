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
class IbisExecutionEngineForceIbisTests(ExecutionEngineTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        # cls._con.close()
        pass

    def make_engine(self):
        return MockDuckExecutionEngine({"test": True}, force_is_ibis=True)

    def test_properties(self):
        assert not self.engine.is_distributed
        assert not self.engine.map_engine.is_distributed
        assert not self.engine.sql_engine.is_distributed

    def test_select(self):
        # it can't work properly with DuckDB (hugeint is not recognized)
        pass

    def test_get_parallelism(self):
        assert self.engine.get_current_parallelism() == 1


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


@pytest.mark.skipif(sys.version_info < (3, 8), reason="< 3.8")
class DuckBuiltInForceIbisTests(BuiltInTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._engine = cls.make_engine(cls)

    @classmethod
    def tearDownClass(cls):
        # cls._con.close()
        pass

    def make_engine(self):
        return MockDuckExecutionEngine({"test": True}, force_is_ibis=True)

    def test_df_select(self):
        # it can't work properly with DuckDB (hugeint is not recognized)
        pass
