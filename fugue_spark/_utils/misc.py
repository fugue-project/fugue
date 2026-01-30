import pyspark.sql as ps
import pyspark
from typing import Any


SPARK_VERSION = pyspark.__version__.split(".")

try:
    if int(SPARK_VERSION[0]) >= 4:
        from pyspark.sql import SparkSession as SparkConnectSession
        from pyspark.sql import DataFrame as SparkConnectDataFrame
    else:  # pragma: no cover
        from pyspark.sql.connect.session import SparkSession as SparkConnectSession
        from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame
except Exception:  # pragma: no cover
    SparkConnectSession = None
    SparkConnectDataFrame = None


def is_spark_connect(session: Any) -> bool:  # pragma: no cover
    if int(SPARK_VERSION[0]) >= 4:
        return False
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
