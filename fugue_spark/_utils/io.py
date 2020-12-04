from typing import Any, Callable, Dict, List, Optional, Union

import pyspark.sql as ps
from fugue.collections.partition import EMPTY_PARTITION_SPEC, PartitionSpec
from fugue.dataframe import DataFrame
from fugue._utils.io import FileParser, save_df
from fugue_spark.dataframe import SparkDataFrame
from fugue_spark._utils.convert import to_schema, to_spark_schema
from pyspark.sql import SparkSession
from triad.collections import Schema
from triad.collections.fs import FileSystem
from triad.exceptions import InvalidOperationError
from triad.utils.assertion import assert_or_throw
from triad.collections.dict import ParamDict


class SparkIO(object):
    def __init__(self, spark_session: SparkSession, fs: FileSystem):
        self._session = spark_session
        self._fs = fs
        self._loads: Dict[str, Callable[..., DataFrame]] = {
            "csv": self._load_csv,
            "parquet": self._load_parquet,
            "json": self._load_json,
        }

    def load_df(
        self,
        uri: Union[str, List[str]],
        format_hint: Optional[str] = None,
        columns: Any = None,
        **kwargs: Any,
    ) -> DataFrame:
        if isinstance(uri, str):
            fp = [FileParser(uri, format_hint)]
        else:
            fp = [FileParser(u, format_hint) for u in uri]
        fmts = list(set(f.file_format for f in fp))  # noqa: C401
        assert_or_throw(
            len(fmts) == 1, NotImplementedError("can't support multiple formats")
        )
        fmt = fmts[0]
        files = [f.uri for f in fp]
        return self._loads[fmt](files, columns, **kwargs)

    def save_df(
        self,
        df: SparkDataFrame,
        uri: str,
        format_hint: Optional[str] = None,
        partition_spec: PartitionSpec = EMPTY_PARTITION_SPEC,
        mode: str = "overwrite",
        force_single: bool = False,
        **kwargs: Any,
    ) -> None:
        if not force_single:
            p = FileParser(uri, format_hint)
            writer = self._get_writer(df.native, partition_spec)
            writer.format(p.file_format).options(**kwargs).mode(mode)
            writer.save(uri)
        else:
            ldf = df.as_local()
            save_df(ldf, uri, format_hint=format_hint, mode=mode, fs=self._fs, **kwargs)

    def _get_writer(
        self, sdf: ps.DataFrame, partition_spec: PartitionSpec
    ) -> ps.DataFrameWriter:
        # will not use bucket by because here we don't save as table
        num = int(partition_spec.num_partitions)  # TODO: this is hacky
        by = partition_spec.partition_by
        if num == 0 and len(by) == 0:
            return sdf.write
        elif num > 0 and len(by) == 0:
            return sdf.repartition(num).write
        elif num == 0 and len(by) > 0:
            return sdf.write.partitionBy(*by)
        return sdf.repartition(num, *by).write.partitionBy(*by)

    def _load_parquet(
        self, p: List[str], columns: Any = None, **kwargs: Any
    ) -> DataFrame:
        sdf = self._session.read.parquet(*p, **kwargs)
        if columns is None:
            return SparkDataFrame(sdf)
        if isinstance(columns, list):  # column names
            return SparkDataFrame(sdf)[columns]
        schema = Schema(columns)
        return SparkDataFrame(sdf[schema.names], schema)

    def _load_csv(self, p: List[str], columns: Any = None, **kwargs: Any) -> DataFrame:
        kw = ParamDict(kwargs)
        infer_schema = kw.get("infer_schema", False)
        if infer_schema:
            kw["inferSchema"] = True
        if "infer_schema" in kw:
            del kw["infer_schema"]
        header = str(kw.get_or_none("header", object)).lower()
        if "header" in kw:
            del kw["header"]
        reader = self._session.read.format("csv")
        reader.options(**kw)
        if header == "true":
            reader.option("header", "true")
            if columns is None:
                return SparkDataFrame(reader.load(p))
            if isinstance(columns, list):  # column names
                return SparkDataFrame(reader.load(p)[columns])
            schema = Schema(columns)
            return SparkDataFrame(reader.load(p)[schema.names], schema)
        if header in ["false", "none"]:
            reader.option("header", "false")
            if columns is None:
                raise InvalidOperationError("columns must be set if without header")
            if isinstance(columns, list):  # column names
                sdf = reader.load(p)
                inferred = to_schema(sdf)
                renames = [f"{k} AS {v}" for k, v in zip(inferred.names, columns)]
                return SparkDataFrame(sdf.selectExpr(*renames))
            schema = Schema(columns)
            sdf = reader.schema(to_spark_schema(schema)).load(p)
            return SparkDataFrame(sdf, schema)
        else:
            raise NotImplementedError(f"{header} is not supported")

    def _load_json(self, p: List[str], columns: Any = None, **kwargs: Any) -> DataFrame:
        reader = self._session.read.format("json")
        reader.options(**kwargs)
        if columns is None:
            return SparkDataFrame(reader.load(p))
        if isinstance(columns, list):  # column names
            return SparkDataFrame(reader.load(p))[columns]
        schema = Schema(columns)
        return SparkDataFrame(reader.load(p)[schema.names], schema)
