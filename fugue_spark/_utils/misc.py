import pyspark.sql as ps
from typing import Any


try:
    from pyspark.sql import is_remote as _is_remote
except ImportError:  # pragma: no cover
    _is_remote = None

try:
    from pyspark.sql.connect.session import SparkSession as SparkConnectSession
    from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame
except Exception:  # pragma: no cover
    SparkConnectSession = None
    SparkConnectDataFrame = None


def is_spark_connect(session: Any) -> bool:  # pragma: no cover
    if _is_remote is not None:
        return _is_remote()
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
