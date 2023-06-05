import pytest
from pyspark.sql import SparkSession

import fugue.api as fa
from fugue_spark.execution_engine import SparkExecutionEngine

from .test_dataframe import NativeSparkDataFrameTests as _NativeDataFrameTests
from .test_dataframe import SparkDataFrameTests as _DataFrameTests
from .test_execution_engine import (
    SparkExecutionEnginePandasUDFBuiltInTests as _WorkflowTests,
)
from .test_execution_engine import SparkExecutionEnginePandasUDFTests as _EngineTests


class SparkConnectDataFrameTests(_DataFrameTests):
    @pytest.fixture(autouse=True)
    def init_session(self):
        self.spark_session = _connect()


class SparkConnectNativeDataFrameTests(_NativeDataFrameTests):
    @pytest.fixture(autouse=True)
    def init_session(self):
        self.spark_session = _connect()


class SparkConnectExecutionEngineTests(_EngineTests):
    @pytest.fixture(autouse=True)
    def init_session(self):
        self.spark_session = _connect()

    def make_engine(self):
        session = _connect()
        e = SparkExecutionEngine(
            session, {"test": True, "fugue.spark.use_pandas_udf": False}
        )
        return e

    def test_get_parallelism(self):
        assert fa.get_current_parallelism() == 200

    def test_using_pandas_udf(self):
        return

    def test_map_with_dict_col(self):
        return  # spark connect has a bug


class SparkConnectBuiltInTests(_WorkflowTests):
    @pytest.fixture(autouse=True)
    def init_session(self):
        self.spark_session = _connect()

    def make_engine(self):
        session = _connect()
        e = SparkExecutionEngine(
            session,
            {
                "test": True,
                "fugue.spark.use_pandas_udf": True,
                "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
                "fugue.rpc.flask_server.host": "127.0.0.1",
                "fugue.rpc.flask_server.port": "1234",
                "fugue.rpc.flask_server.timeout": "2 sec",
                "spark.sql.shuffle.partitions": "10",
            },
        )
        assert e.conf.get_or_throw("fugue.spark.use_pandas_udf", bool)
        return e

    def test_annotation_3(self):
        return  # RDD is not implemented in spark connect

    def test_repartition(self):
        return  # spark connect doesn't support even repartitioning

    def test_repartition_large(self):
        return  # spark connect doesn't support even repartitioning


def _connect():
    return SparkSession.builder.remote("sc://localhost").getOrCreate()
