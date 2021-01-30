import json
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
from fugue.dataframe import (
    IterableDataFrame,
    PandasDataFrame,
    ArrayDataFrame,
    LocalDataFrameIterableDataFrame,
)
from fugue.dataframe.utils import _df_eq as df_eq
from fugue.exceptions import FugueDataFrameInitError
from fugue_test.dataframe_suite import DataFrameTests
from pytest import raises
from triad.collections.schema import Schema, SchemaError
from triad.exceptions import InvalidOperationError


class LocalDataFrameIterableDataFrameTests(DataFrameTests.Tests):
    def df(
        self, data: Any = None, schema: Any = None, metadata: Any = None
    ) -> IterableDataFrame:
        def get_dfs():
            if isinstance(data, list):
                for row in data:
                    yield ArrayDataFrame([], schema, metadata)  # noise
                    yield ArrayDataFrame([row], schema, metadata)
                if schema is None:
                    yield ArrayDataFrame([], schema, metadata)  # noise
            elif data is not None:
                yield ArrayDataFrame(data, schema, metadata)

        return LocalDataFrameIterableDataFrame(get_dfs(), schema, metadata)