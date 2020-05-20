from typing import Any, Iterable, List, Tuple, no_type_check

from fugue.collections.partition import PartitionCursor
from fugue.dataframe import (
    ArrayDataFrame,
    DataFrame,
    DataFrames,
    IterableDataFrame,
    to_local_bounded_df,
)
from fugue.exceptions import FugueWorkflowError
from fugue.execution import SQLEngine
from fugue.extensions.processor import Processor
from fugue.extensions.transformer import CoTransformer, Transformer, to_transformer
from triad.collections import ParamDict
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import to_instance, to_type
from triad.utils.iter import EmptyAwareIterable


class RunTransformer(Processor):
    @no_type_check
    def process(self, dfs: DataFrames) -> DataFrame:
        df = dfs[0]
        tf = to_transformer(
            self.params.get_or_none("transformer", object),
            self.params.get_or_none("schema", object),
        )
        tf._workflow_conf = self.execution_engine.conf
        tf._params = self.params.get("params", ParamDict())  # type: ignore
        tf._partition_spec = self.partition_spec  # type: ignore
        ie = self.params.get("ignore_errors", [])
        self._ignore_errors = [to_type(x, Exception) for x in ie]

        if isinstance(tf, Transformer):
            return self.transform(df, tf)
        else:
            return self.cotransform(df, tf)

    def transform(self, df: DataFrame, tf: Transformer) -> DataFrame:
        tf._key_schema = self.partition_spec.get_key_schema(df.schema)  # type: ignore
        tf._output_schema = Schema(tf.get_output_schema(df))  # type: ignore
        tr = _TransformerRunner(df, tf, self._ignore_errors)  # type: ignore
        return self.execution_engine.map_partitions(
            df=df,
            mapFunc=tr.run,
            output_schema=tf.output_schema,  # type: ignore
            partition_spec=tf.partition_spec,
        )

    @no_type_check
    def cotransform(self, df: DataFrame, tf: CoTransformer) -> DataFrame:
        assert_or_throw(
            df.metadata.get("serialized", False), "must use serialized dataframe"
        )
        tf._key_schema = df.schema - list(df.metadata["serialized_cols"].values())
        # TODO: currently, get_output_schema only gets empty dataframes
        empty_dfs = DataFrames(
            {k: ArrayDataFrame([], v) for k, v in df.metadata["schemas"].items()}
        )
        tf._output_schema = Schema(tf.get_output_schema(empty_dfs))  # type: ignore
        tr = _CoTransformerRunner(df, tf, self._ignore_errors)  # type: ignore
        return self.execution_engine.comap_serialized(
            df=df,
            mapFunc=tr.run,
            output_schema=tf.output_schema,  # type: ignore
            partition_spec=tf.partition_spec,
        )


class RunJoin(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        if len(dfs) == 1:
            return dfs[0]
        how = self.params.get_or_throw("how", str)
        on = self.params.get("on", [])
        df = dfs[0]
        for i in range(1, len(dfs)):
            df = self.execution_engine.join(df, dfs[i], how=how, on=on)
        return df


class RunSQLSelect(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        statement = self.params.get_or_throw("statement", str)
        engine = self.params.get_or_none("sql_engine", object)
        if engine is None:
            engine = self.execution_engine.default_sql_engine
        elif not isinstance(engine, SQLEngine):
            engine = to_instance(engine, SQLEngine, args=[self.execution_engine])
        return engine.select(dfs, statement)


class Zip(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        how = self.params.get("how", "inner")
        partition_spec = self.partition_spec
        # TODO: this should also search on workflow conf
        temp_path = self.params.get_or_none("temp_path", str)
        to_file_threshold = self.params.get_or_none("to_file_threshold", object)
        if to_file_threshold is None:  # pragma: no cover
            to_file_threshold = -1
        if len(dfs) == 1:
            return self.execution_engine.serialize_by_partition(
                dfs[0], partition_spec, "_0", temp_path, to_file_threshold
            )
        df = self.execution_engine.zip_dataframes(
            dfs[0],
            dfs[1],
            how=how,
            partition_spec=partition_spec,
            temp_path=temp_path,
            to_file_threshold=to_file_threshold,
        )
        for i in range(2, len(dfs)):
            df = self.execution_engine.zip_dataframes(
                df,
                dfs[i],
                how=how,
                partition_spec=partition_spec,
                temp_path=temp_path,
                to_file_threshold=to_file_threshold,
            )
        return df


class Rename(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        columns = self.params.get_or_throw("columns", dict)
        return dfs[0].rename(columns)


class DropColumns(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        if_exists = self.params.get("if_exists", False)
        columns = self.params.get_or_throw("columns", list)
        if if_exists:
            columns = set(columns).intersection(dfs[0].schema.keys())
        return dfs[0].drop(list(columns))


class SelectColumns(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        columns = self.params.get_or_throw("columns", list)
        return dfs[0][columns]


class _TransformerRunner(object):
    def __init__(
        self, df: DataFrame, transformer: Transformer, ignore_errors: List[type]
    ):
        self.schema = df.schema
        self.metadata = df.metadata
        self.transformer = transformer
        self.ignore_errors = tuple(ignore_errors)

    def run(self, no: int, data: Iterable[Any]) -> Iterable[Any]:
        df = IterableDataFrame(data, self.schema, self.metadata)
        if df.empty:  # pragma: no cover
            return
        spec = self.transformer.partition_spec
        self.transformer._cursor = spec.get_cursor(  # type: ignore
            self.schema, no
        )
        self.transformer.init_physical_partition(df)
        if spec.empty:
            partitions: Iterable[Tuple[int, int, EmptyAwareIterable]] = [
                (0, 0, df.native)
            ]
        else:
            partitioner = spec.get_partitioner(self.schema)
            partitions = partitioner.partition(df.native)
        for pn, sn, sub in partitions:
            self.transformer.cursor.set(sub.peek(), pn, sn)
            sub_df = IterableDataFrame(sub, self.schema)
            sub_df._metadata = self.metadata
            self.transformer.init_logical_partition(sub_df)
            if len(self.ignore_errors) == 0:
                res = self.transformer.transform(sub_df)
                for r in res.as_array_iterable(type_safe=True):
                    yield r
            else:
                try:
                    res = to_local_bounded_df(self.transformer.transform(sub_df))
                except self.ignore_errors:  # type: ignore
                    continue
                for r in res.as_array_iterable(type_safe=True):
                    yield r


class _CoTransformerRunner(object):
    def __init__(
        self, df: DataFrame, transformer: CoTransformer, ignore_errors: List[type]
    ):
        self.schema = df.schema
        self.metadata = df.metadata
        self.transformer = transformer
        self.ignore_errors = tuple(ignore_errors)

    def run(self, cursor: PartitionCursor, dfs: DataFrames) -> Iterable[Any]:
        self.transformer._cursor = cursor  # type: ignore
        self.transformer.init_physical_partition(dfs)
        if len(self.ignore_errors) == 0:
            res = self.transformer.transform(dfs)
            for r in res.as_array_iterable(type_safe=True):
                yield r
        else:
            try:
                res = to_local_bounded_df(self.transformer.transform(dfs))
            except self.ignore_errors:  # type: ignore
                return
            for r in res.as_array_iterable(type_safe=True):
                yield r
