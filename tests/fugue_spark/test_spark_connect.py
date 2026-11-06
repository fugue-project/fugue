import os

import fugue.test as ft
from fugue_spark._utils.misc import is_spark_connect
from fugue_spark.execution_engine import SparkExecutionEngine

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
    def test_get_parallelism(self):
        assert self.engine.get_current_parallelism() > 0

    def test_get_parallelism_with_unavailable_configs(self, mocker):
        mocker.patch.object(
            type(self.spark_session.conf),
            "get",
            side_effect=RuntimeError("configuration is not available"),
        )
        assert self.engine.get_current_parallelism() == 200

    def test_spark_connect_detection(self):
        expected_version = os.environ.get("FUGUE_SPARK_VERSION")
        if expected_version is not None:
            assert self.spark_session.version == expected_version
        assert is_spark_connect(self.spark_session)
        assert is_spark_connect(self.spark_session.range(1))
        assert SparkExecutionEngine(self.spark_session).is_spark_connect

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
