# flake8: noqa
from importlib.metadata import version

__version__ = version("fugue")

from fugue_spark.dataframe import SparkDataFrame
from fugue_spark.execution_engine import SparkExecutionEngine
