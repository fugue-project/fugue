from typing import List, no_type_check

from triad import ParamDict, Schema, SerializableRLock, assert_or_throw
from triad.utils.convert import to_type

from fugue.collections.partition import PartitionCursor
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.utils import _df_eq
from fugue.exceptions import FugueWorkflowError
from fugue.execution.execution_engine import _generate_comap_empty_dfs
from fugue.rpc import EmptyRPCHandler, to_rpc_handler

from ..outputter import Outputter
from ..transformer.convert import _to_output_transformer
from ..transformer.transformer import CoTransformer, Transformer


class Show(Outputter):
    LOCK = SerializableRLock()

    def process(self, dfs: DataFrames) -> None:
        title = self.params.get_or_none("title", object)
        title = str(title) if title is not None else None
        n = self.params.get("n", 10)
        with_count = self.params.get("with_count", False)
        with Show.LOCK:
            m = 0
            for df in dfs.values():
                df.show(n=n, with_count=with_count, title=title if m == 0 else None)
                m += 1


class AssertEqual(Outputter):
    def process(self, dfs: DataFrames) -> None:
        assert_or_throw(len(dfs) > 1, FugueWorkflowError("can't accept single input"))
        expected = dfs[0]
        for i in range(1, len(dfs)):
            _df_eq(expected, dfs[i], throw=True, **self.params)


class AssertNotEqual(Outputter):
    def process(self, dfs: DataFrames) -> None:
        assert_or_throw(len(dfs) > 1, FugueWorkflowError("can't accept single input"))
        expected = dfs[0]
        for i in range(1, len(dfs)):
            assert not _df_eq(expected, dfs[i], throw=False, **self.params)


class Save(Outputter):
    def process(self, dfs: DataFrames) -> None:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        kwargs = self.params.get("params", dict())
        path = self.params.get_or_throw("path", str)
        format_hint = self.params.get("fmt", "")
        mode = self.params.get("mode", "overwrite")
        partition_spec = self.partition_spec
        force_single = self.params.get("single", False)

        self.execution_engine.save_df(
            df=dfs[0],
            path=path,
            format_hint=format_hint,
            mode=mode,
            partition_spec=partition_spec,
            force_single=force_single,
            **kwargs
        )


class RunOutputTransformer(Outputter):
    @no_type_check
    def process(self, dfs: DataFrames) -> None:
        df = dfs[0]
        tf = _to_output_transformer(
            self.params.get_or_none("transformer", object),
        )
        tf._workflow_conf = self.execution_engine.conf
        tf._params = self.params.get("params", ParamDict())  # type: ignore
        tf._partition_spec = self.partition_spec  # type: ignore
        rpc_handler = to_rpc_handler(self.params.get_or_throw("rpc_handler", object))
        if not isinstance(rpc_handler, EmptyRPCHandler):
            tf._rpc_client = self.rpc_server.make_client(rpc_handler)
        ie = self.params.get("ignore_errors", [])
        self._ignore_errors = [to_type(x, Exception) for x in ie]
        tf.validate_on_runtime(df)
        if isinstance(tf, Transformer):
            self.transform(df, tf)
        else:
            self.cotransform(df, tf)

    def transform(self, df: DataFrame, tf: Transformer) -> None:
        tf._key_schema = self.partition_spec.get_key_schema(df.schema)  # type: ignore
        tf._output_schema = Schema(tf.get_output_schema(df))  # type: ignore
        tr = _TransformerRunner(df, tf, self._ignore_errors)  # type: ignore
        df = self.execution_engine.map_engine.map_dataframe(
            df=df,
            map_func=tr.run,
            output_schema=tf.output_schema,  # type: ignore
            partition_spec=tf.partition_spec,
            on_init=tr.on_init,
            map_func_format_hint=tf.get_format_hint(),
        )
        self.execution_engine.persist(df, lazy=False)

    @no_type_check
    def cotransform(self, df: DataFrame, tf: CoTransformer) -> None:
        assert_or_throw(
            df.metadata.get("serialized", False), "must use serialized dataframe"
        )
        tf._key_schema = df.schema - list(df.metadata["serialized_cols"].values())
        # TODO: currently, get_output_schema only gets empty dataframes
        empty_dfs = _generate_comap_empty_dfs(
            df.metadata["schemas"], df.metadata.get("serialized_has_name", False)
        )
        tf._output_schema = Schema(tf.get_output_schema(empty_dfs))
        tr = _CoTransformerRunner(df, tf, self._ignore_errors)
        df = self.execution_engine.comap(
            df=df,
            map_func=tr.run,
            output_schema=tf.output_schema,
            partition_spec=tf.partition_spec,
            on_init=tr.on_init,
        )
        self.execution_engine.persist(df, lazy=False)


class _TransformerRunner(object):
    def __init__(
        self, df: DataFrame, transformer: Transformer, ignore_errors: List[type]
    ):
        self.schema = df.schema
        self.transformer = transformer
        self.ignore_errors = tuple(ignore_errors)

    def run(self, cursor: PartitionCursor, df: LocalDataFrame) -> LocalDataFrame:
        self.transformer._cursor = cursor  # type: ignore
        try:
            self.transformer.transform(df).as_local_bounded()
            return ArrayDataFrame([], self.transformer.output_schema)
        except self.ignore_errors:  # type: ignore
            return ArrayDataFrame([], self.transformer.output_schema)

    def on_init(self, partition_no: int, df: DataFrame) -> None:
        s = self.transformer.partition_spec
        self.transformer._cursor = s.get_cursor(  # type: ignore
            self.schema, partition_no
        )
        self.transformer.on_init(df)


class _CoTransformerRunner(object):
    def __init__(
        self, df: DataFrame, transformer: CoTransformer, ignore_errors: List[type]
    ):
        self.schema = df.schema
        self.transformer = transformer
        self.ignore_errors = tuple(ignore_errors)

    def run(self, cursor: PartitionCursor, dfs: DataFrames) -> LocalDataFrame:
        self.transformer._cursor = cursor  # type: ignore
        try:
            self.transformer.transform(dfs).as_local_bounded()
            return ArrayDataFrame([], self.transformer.output_schema)
        except self.ignore_errors:  # type: ignore
            return ArrayDataFrame([], self.transformer.output_schema)

    def on_init(self, partition_no: int, dfs: DataFrames) -> None:
        s = self.transformer.partition_spec
        self.transformer._cursor = s.get_cursor(  # type: ignore
            self.schema, partition_no
        )
        self.transformer.on_init(dfs)
