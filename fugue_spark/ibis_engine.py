from typing import Any, Callable

import ibis
from pyspark.sql import DataFrame as PySparkDataFrame
from triad.utils.assertion import assert_or_throw

from fugue import DataFrame, DataFrames, ExecutionEngine
from fugue_ibis import IbisTable
from fugue_ibis._utils import to_schema
from fugue_ibis.execution.ibis_engine import IbisEngine, parse_ibis_engine
from fugue_spark.dataframe import SparkDataFrame
from fugue_spark.execution_engine import SparkExecutionEngine


class SparkIbisEngine(IbisEngine):
    def __init__(self, execution_engine: ExecutionEngine) -> None:
        assert_or_throw(
            isinstance(execution_engine, SparkExecutionEngine),
            lambda: ValueError(
                f"SparkIbisEngine must use SparkExecutionEngine ({execution_engine})"
            ),
        )
        super().__init__(execution_engine)

    def select(
        self, dfs: DataFrames, ibis_func: Callable[[ibis.BaseBackend], IbisTable]
    ) -> DataFrame:
        for k, v in dfs.items():
            self.execution_engine.register(v, k)  # type: ignore
        con = ibis.pyspark.connect(self.execution_engine.spark_session)  # type: ignore
        expr = ibis_func(con)
        schema = to_schema(expr.schema())
        result = expr.compile()
        assert_or_throw(
            isinstance(result, PySparkDataFrame),
            lambda: ValueError(f"result must be a PySpark DataFrame ({type(result)})"),
        )
        return SparkDataFrame(result, schema=schema)


@parse_ibis_engine.candidate(
    lambda obj, *args, **kwargs: isinstance(obj, SparkExecutionEngine)
)
def _spark_to_ibis_engine(obj: Any, engine: ExecutionEngine) -> IbisEngine:
    return SparkIbisEngine(engine)
