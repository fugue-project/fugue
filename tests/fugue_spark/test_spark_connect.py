import fugue.test as ft

from .test_dataframe import NativeSparkDataFrameTestsBase as _NativeDataFrameTests
from .test_dataframe import SparkDataFrameTestsBase as _DataFrameTests
from .test_execution_engine import _CONF
from .test_execution_engine import (
    SparkExecutionEngineBuiltInTestsBase as _WorkflowTests,
)
from .test_execution_engine import (
    SparkExecutionEnginePandasUDFTestsBase as _EngineTests,
)


@ft.fugue_test_suite("sparkconnect", mark_test=True)
class SparkConnectDataFrameTests(_DataFrameTests):
    pass


@ft.fugue_test_suite("sparkconnect", mark_test=True)
class SparkConnectNativeDataFrameTests(_NativeDataFrameTests):
    pass


@ft.fugue_test_suite("sparkconnect", mark_test=True)
class SparkConnectExecutionEngineTests(_EngineTests):
    def test_using_pandas_udf(self):
        return

    def test_map_with_dict_col(self):
        return  # spark connect has a bug


@ft.fugue_test_suite(("sparkconnect", _CONF), mark_test=True)
class SparkConnectBuiltInTests(_WorkflowTests):
    def test_annotation_3(self):
        return  # RDD is not implemented in spark connect

    def test_repartition(self):
        return  # spark connect doesn't support even repartitioning

    def test_repartition_large(self):
        return  # spark connect doesn't support even repartitioning
