from typing import Dict, Any

FUGUE_SPARK_CONF_USE_PANDAS_UDF = "fugue.spark.use_pandas_udf"

FUGUE_SPARK_DEFAULT_CONF: Dict[str, Any] = {FUGUE_SPARK_CONF_USE_PANDAS_UDF: True}
