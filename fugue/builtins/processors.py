from typing import Any, Iterable, List, Tuple

from fugue.dataframe import DataFrame, DataFrames, IterableDataFrame
from fugue.dataframe.utils import to_local_bounded_df
from fugue.processor import Processor
from fugue.transformer import Transformer, to_transformer
from triad.collections import ParamDict
from triad.utils.convert import to_type
from triad.utils.iter import EmptyAwareIterable


class RunTransformer(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        df = dfs[0]
        tf = to_transformer(
            self.params.get_or_none("transformer", object),
            self.params.get_or_none("schema", object),
        )
        tf._params = self.params.get("params", ParamDict())  # type: ignore
        tf._partition_spec = self.pre_partition  # type: ignore
        tf._key_schema = self.pre_partition.get_key_schema(df.schema)  # type: ignore
        tf._output_schema = tf.get_output_schema(df)  # type: ignore
        ie = self.params.get("ignore_errors", [])
        ignore_errors = [to_type(x, Exception) for x in ie]
        tr = _TransformerRunner(df, tf, ignore_errors)  # type: ignore
        return self.execution_engine.map_partitions(
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
        keys = self.params.get("keys", [])
        df = dfs[0]
        for i in range(1, len(dfs)):
            df = self.execution_engine.join(df, dfs[i], how=how, keys=keys)
        return df


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
