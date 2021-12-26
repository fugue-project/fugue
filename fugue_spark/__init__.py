# flake8: noqa
from fugue_version import __version__

from fugue_spark.dataframe import SparkDataFrame
from fugue_spark.execution_engine import SparkExecutionEngine
from fugue_spark.registry import register

try:
    from fugue_spark.ibis_engine import SparkIbisEngine
except Exception:  # pragma: no cover
    pass

register()
