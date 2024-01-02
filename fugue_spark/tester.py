from contextlib import contextmanager
from typing import Any, Dict, Iterator

from pyspark.sql import SparkSession

import fugue.test as ft

from ._utils.misc import SparkConnectSession


@ft.fugue_test_backend
class SparkTestBackend(ft.FugueTestBackend):
    name = "spark"
    default_session_conf = {
        "spark.app.name": "fugue-test-spark",
        "spark.master": "local[*]",
        "spark.default.parallelism": 4,
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.cores": 4,
        "spark.executor.instances": 1,
        "spark.io.compression.codec": "lz4",
        "spark.rdd.compress": "false",
        "spark.sql.shuffle.partitions": 4,
        "spark.shuffle.compress": "false",
        "spark.sql.catalogImplementation": "in-memory",
        "spark.sql.execution.arrow.pyspark.enabled": True,
        "spark.sql.adaptive.enabled": False,
    }

    @classmethod
    def transform_session_conf(cls, conf: Dict[str, Any]) -> Dict[str, Any]:
        return ft.extract_conf(conf, "spark.", remove_prefix=False)

    @classmethod
    @contextmanager
    def session_context(cls, session_conf: Dict[str, Any]) -> Iterator[Any]:
        with _create_session(session_conf).getOrCreate() as spark:
            yield spark


if SparkConnectSession is not None:

    @ft.fugue_test_backend
    class SparkConnectTestBackend(SparkTestBackend):
        name = "sparkconnect"
        default_session_conf = {
            "spark.default.parallelism": 4,
            "spark.sql.shuffle.partitions": 4,
            "spark.sql.execution.arrow.pyspark.enabled": True,
            "spark.sql.adaptive.enabled": False,
        }

        @classmethod
        def transform_session_conf(
            cls, conf: Dict[str, Any]
        ) -> Dict[str, Any]:  # pragma: no cover
            # replace sparkconnect. with spark.
            return {
                "spark." + k: v
                for k, v in ft.extract_conf(
                    conf, cls.name + ".", remove_prefix=True
                ).items()
            }

        @classmethod
        @contextmanager
        def session_context(
            cls, session_conf: Dict[str, Any]
        ) -> Iterator[Any]:  # pragma: no cover
            spark = _create_session(session_conf).remote("sc://localhost").getOrCreate()
            yield spark


def _create_session(conf: Dict[str, Any]) -> Any:
    sb = SparkSession.builder
    for k, v in conf.items():
        sb = sb.config(k, v)
    return sb
