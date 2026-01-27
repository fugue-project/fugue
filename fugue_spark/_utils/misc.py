from typing import Any
from pyspark import __version__

try:
    if __version__ >= "4.0.0":
        from pyspark.sql import SparkSession as SparkConnectSession
        from pyspark.sql import DataFrame as SparkConnectDataFrame
    else:
        from pyspark.sql.connect.session import SparkSession as SparkConnectSession
        from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame
except Exception:  # pragma: no cover
    SparkConnectSession = None
    SparkConnectDataFrame = None
import pyspark.sql as ps


def is_spark_connect(session: Any) -> bool:
    return SparkConnectSession is not None and isinstance(
        session, (SparkConnectSession, SparkConnectDataFrame)
    )


def is_spark_dataframe(df: Any) -> bool:
    return isinstance(df, ps.DataFrame) or (
        SparkConnectDataFrame is not None and isinstance(df, SparkConnectDataFrame)
    )


def is_spark_session(session: Any) -> bool:
    return isinstance(session, ps.SparkSession) or (
        SparkConnectSession is not None and isinstance(session, SparkConnectSession)
    )
