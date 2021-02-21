from typing import Any, List, Type, no_type_check

from fugue.collections.partition import PartitionCursor
from fugue.dataframe import (
    ArrayDataFrame,
    DataFrame,
    DataFrames,
    LocalDataFrame,
    to_local_bounded_df,
)
from fugue.exceptions import FugueWorkflowError
from fugue.execution import make_sql_engine
from fugue.execution.execution_engine import _generate_comap_empty_dfs
from fugue.extensions.processor import Processor
from fugue.extensions.transformer import CoTransformer, Transformer, _to_transformer
from fugue.rpc import EmptyRPCHandler, to_rpc_handler
from triad.collections import ParamDict
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import to_type


class RunTransformer(Processor):
    @no_type_check
    def process(self, dfs: DataFrames) -> DataFrame:
        df = dfs[0]
        tf = _to_transformer(
            self.params.get_or_none("transformer", object),
            self.params.get_or_none("schema", object),
        )
        tf._workflow_conf = self.execution_engine.conf
        tf._params = self.params.get("params", ParamDict())  # type: ignore
        tf._partition_spec = self.partition_spec
        rpc_handler = to_rpc_handler(self.params.get_or_throw("rpc_handler", object))
        if not isinstance(rpc_handler, EmptyRPCHandler):
            tf._rpc_client = self.execution_engine.rpc_server.make_client(rpc_handler)
        ie = self.params.get("ignore_errors", [])
        self._ignore_errors = [to_type(x, Exception) for x in ie]
        tf.validate_on_runtime(df)
        if isinstance(tf, Transformer):
            return self.transform(df, tf)
        else:
            return self.cotransform(df, tf)

    def transform(self, df: DataFrame, tf: Transformer) -> DataFrame:
        tf._key_schema = self.partition_spec.get_key_schema(df.schema)  # type: ignore
        tf._output_schema = Schema(tf.get_output_schema(df))  # type: ignore
        tr = _TransformerRunner(df, tf, self._ignore_errors)  # type: ignore
        return self.execution_engine.map(
            df=df,
            map_func=tr.run,
            output_schema=tf.output_schema,  # type: ignore
            partition_spec=tf.partition_spec,
            on_init=tr.on_init,
        )

    @no_type_check
    def cotransform(self, df: DataFrame, tf: CoTransformer) -> DataFrame:
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
        return self.execution_engine.comap(
            df=df,
            map_func=tr.run,
            output_schema=tf.output_schema,
            partition_spec=tf.partition_spec,
            on_init=tr.on_init,
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


class RunSetOperation(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        if len(dfs) == 1:
            return dfs[0]
        how = self.params.get_or_throw("how", str)
        func: Any = {
            "union": self.execution_engine.union,
            "subtract": self.execution_engine.subtract,
            "intersect": self.execution_engine.intersect,
        }[how]
        distinct = self.params.get("distinct", True)
        df = dfs[0]
        for i in range(1, len(dfs)):
            df = func(df, dfs[i], distinct=distinct)
        return df


class Distinct(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        return self.execution_engine.distinct(dfs[0])


class Dropna(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        how = self.params.get("how", "any")
        assert_or_throw(
            how in ["any", "all"],
            FugueWorkflowError("how' needs to be either 'any' or 'all'"),
        )
        thresh = self.params.get_or_none("thresh", int)
        subset = self.params.get_or_none("subset", list)
        return self.execution_engine.dropna(
            dfs[0], how=how, thresh=thresh, subset=subset
        )


class Fillna(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        value = self.params.get_or_none("value", object)
        assert_or_throw(
            (not isinstance(value, list)) and (value is not None),
            FugueWorkflowError("fillna value cannot be None or list"),
        )
        if isinstance(value, dict):
            assert_or_throw(
                (None not in value.values()) and (any(value.values())),
                FugueWorkflowError(
                    "fillna dict can't contain None and must have len > 1"
                ),
            )
        subset = self.params.get_or_none("subset", list)
        return self.execution_engine.fillna(dfs[0], value=value, subset=subset)


class RunSQLSelect(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        statement = self.params.get_or_throw("statement", str)
        engine = self.params.get_or_none("sql_engine", object)
        engine_params = self.params.get("sql_engine_params", ParamDict())
        sql_engine = make_sql_engine(engine, self.execution_engine, **engine_params)
        return sql_engine.select(dfs, statement)


class Zip(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        how = self.params.get("how", "inner")
        partition_spec = self.partition_spec
        # TODO: this should also search on workflow conf
        temp_path = self.params.get_or_none("temp_path", str)
        to_file_threshold = self.params.get_or_none("to_file_threshold", object)
        return self.execution_engine.zip_all(
            dfs,
            how=how,
            partition_spec=partition_spec,
            temp_path=temp_path,
            to_file_threshold=to_file_threshold,
        )


class Rename(Processor):
    def validate_on_compile(self):
        self.params.get_or_throw("columns", dict)

    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        columns = self.params.get_or_throw("columns", dict)
        return dfs[0].rename(columns)


class AlterColumns(Processor):
    def validate_on_compile(self):
        Schema(self.params.get_or_throw("columns", object))

    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        columns = self.params.get_or_throw("columns", object)
        return dfs[0].alter_columns(columns)


class DropColumns(Processor):
    def validate_on_compile(self):
        self.params.get_or_throw("columns", list)

    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        if_exists = self.params.get("if_exists", False)
        columns = self.params.get_or_throw("columns", list)
        if if_exists:
            columns = set(columns).intersection(dfs[0].schema.keys())
        return dfs[0].drop(list(columns))


class SelectColumns(Processor):
    def validate_on_compile(self):
        self.params.get_or_throw("columns", list)

    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        columns = self.params.get_or_throw("columns", list)
        return dfs[0][columns]


class Sample(Processor):
    def validate_on_compile(self):
        n = self.params.get_or_none("n", int)
        frac = self.params.get_or_none("frac", float)
        assert_or_throw(
            (n is None and frac is not None) or (n is not None and frac is None),
            ValueError("one and only one of n and frac should be set"),
        )

    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        n = self.params.get_or_none("n", int)
        frac = self.params.get_or_none("frac", float)
        replace = self.params.get("replace", False)
        seed = self.params.get_or_none("seed", int)
        return self.execution_engine.sample(
            dfs[0], n=n, frac=frac, replace=replace, seed=seed
        )


class Take(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
        assert_or_throw(len(dfs) == 1, FugueWorkflowError("not single input"))
        # All _get_or operations convert float to int
        n = self.params.get_or_none("n", int)
        presort = self.params.get_or_none("presort", str)
        na_position = self.params.get("na_position", "last")
        partition_spec = self.partition_spec
        return self.execution_engine.take(
            dfs[0],
            n,
            presort=presort,
            na_position=na_position,
            partition_spec=partition_spec,
        )


class SaveAndUse(Processor):
    def process(self, dfs: DataFrames) -> DataFrame:
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
        return self.execution_engine.load_df(path=path, format_hint=format_hint)


class _TransformerRunner(object):
    def __init__(
        self, df: DataFrame, transformer: Transformer, ignore_errors: List[type]
    ):
        self.schema = df.schema
        self.metadata = df.metadata
        self.transformer = transformer
        self.ignore_errors = tuple(ignore_errors)

    def run(self, cursor: PartitionCursor, df: LocalDataFrame) -> LocalDataFrame:
        self.transformer._cursor = cursor  # type: ignore
        df._metadata = self.metadata
        if len(self.ignore_errors) == 0:
            return self.transformer.transform(df)
        else:
            try:
                return to_local_bounded_df(self.transformer.transform(df))
            except self.ignore_errors:  # type: ignore  # pylint: disable=E0712
                return ArrayDataFrame([], self.transformer.output_schema)

    def on_init(self, partition_no: int, df: DataFrame) -> None:
        s = self.transformer.partition_spec
        self.transformer._cursor = s.get_cursor(  # type: ignore
            self.schema, partition_no
        )
        df._metadata = self.metadata
        self.transformer.on_init(df)


class _CoTransformerRunner(object):
    def __init__(
        self,
        df: DataFrame,
        transformer: CoTransformer,
        ignore_errors: List[Type[Exception]],
    ):
        self.schema = df.schema
        self.metadata = df.metadata
        self.transformer = transformer
        self.ignore_errors = tuple(ignore_errors)

    def run(self, cursor: PartitionCursor, dfs: DataFrames) -> LocalDataFrame:
        self.transformer._cursor = cursor  # type: ignore
        if len(self.ignore_errors) == 0:
            return self.transformer.transform(dfs)

        else:
            try:
                return to_local_bounded_df(self.transformer.transform(dfs))
            except self.ignore_errors:  # type: ignore  # pylint: disable=E0712
                return ArrayDataFrame([], self.transformer.output_schema)

    def on_init(self, partition_no: int, dfs: DataFrames) -> None:
        s = self.transformer.partition_spec
        self.transformer._cursor = s.get_cursor(  # type: ignore
            self.schema, partition_no
        )
        self.transformer.on_init(dfs)
