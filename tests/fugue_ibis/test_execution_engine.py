import sys

import pytest

import fugue.test as ft
from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests

from .mock.tester import mockibisduck_session  # noqa: F401  # pylint: disable-all


@ft.fugue_test_suite("mockibisduck", mark_test=True)
class IbisExecutionEngineTests(ExecutionEngineTests.Tests):
    def test_select(self):
        # it can't work properly with DuckDB (hugeint is not recognized)
        pass


@ft.fugue_test_suite(("mockibisduck", {"fugue.force_is_ibis": True}), mark_test=True)
class IbisExecutionEngineForceIbisTests(ExecutionEngineTests.Tests):
    def test_properties(self):
        assert not self.engine.is_distributed
        assert not self.engine.map_engine.is_distributed
        assert not self.engine.sql_engine.is_distributed

    def test_select(self):
        # it can't work properly with DuckDB (hugeint is not recognized)
        pass

    def test_get_parallelism(self):
        assert self.engine.get_current_parallelism() == 1


@ft.fugue_test_suite("mockibisduck", mark_test=True)
class DuckBuiltInTests(BuiltInTests.Tests):
    def test_df_select(self):
        # it can't work properly with DuckDB (hugeint is not recognized)
        pass


@ft.fugue_test_suite(("mockibisduck", {"fugue.force_is_ibis": True}), mark_test=True)
class DuckBuiltInForceIbisTests(BuiltInTests.Tests):
    def test_df_select(self):
        # it can't work properly with DuckDB (hugeint is not recognized)
        pass
