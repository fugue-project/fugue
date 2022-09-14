from datetime import datetime
from typing import Any

import duckdb
import numpy as np
import pandas as pd
from fugue import (
    ArrowDataFrame,
    DataFrame,
    LocalDataFrame,
    PandasDataFrame,
    IterableDataFrame,
)
import ibis.expr.types as ir
from fugue.dataframe.utils import _df_eq as df_eq
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises
import ibis
from fugue_duckdb.dataframe import DuckDataFrame
from fugue_ibis.dataframe import IbisDataFrame
from fugue_ibis._utils import to_schema


class TestPandasIbisDataFrame(IbisDataFrame):
    def _to_new_df(self, table: ir.Table, metadata: Any = None) -> DataFrame:
        return TestPandasIbisDataFrame(table, metadata)

    def _to_local_df(self, table: ir.Table, metadata: Any = None) -> LocalDataFrame:
        return PandasDataFrame(
            table.execute(), to_schema(table.schema()), metadata=metadata
        )

    def _to_iterable_df(
        self, table: ir.Table, metadata: Any = None
    ) -> IterableDataFrame:
        return PandasDataFrame(
            table.execute().values.tolist(),
            to_schema(table.schema()),
            metadata=metadata,
        )


class IbisDataFrameTests(DataFrameTests.Tests):
    @classmethod
    def setUpClass(cls):
        cls._con = ibis.pandas.connect({})

    def df(
        self, data: Any = None, schema: Any = None, metadata: Any = None
    ) -> DuckDataFrame:
        df = ArrowDataFrame(data, schema, metadata)
        tdf = df.as_pandas()
        name = f"_{id(tdf)}"
        tb = self._con.from_dataframe(tdf, name)
        return TestPandasIbisDataFrame(tb, schema=schema, metadata=metadata)

    def test_map_type(self):
        pass

    def test_deep_nested_types(self):
        pass
