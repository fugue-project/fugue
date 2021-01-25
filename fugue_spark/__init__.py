# flake8: noqa
from fugue.workflow import register_raw_df_type
from fugue_version import __version__
from pyspark.sql import DataFrame

from fugue_spark.dataframe import SparkDataFrame
from fugue_spark.execution_engine import SparkExecutionEngine

register_raw_df_type(DataFrame)
