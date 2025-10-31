import inspect
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    no_type_check,
)

import pandas as pd
import pyarrow as pa
from triad import Schema, assert_or_throw
from triad.collections.function_wrapper import (
    AnnotatedParam,
    FunctionWrapper,
    KeywordParam,
    PositionalParam,
    function_wrapper,
)
from triad.utils.convert import compare_annotations
from triad.utils.iter import EmptyAwareIterable, make_empty_aware

from ..constants import FUGUE_ENTRYPOINT
from ..dataset.api import count as df_count
from .array_dataframe import ArrayDataFrame
from .arrow_dataframe import ArrowDataFrame
from .dataframe import AnyDataFrame, DataFrame, LocalDataFrame, as_fugue_df
from .dataframe_iterable_dataframe import (
    IterableArrowDataFrame,
    IterablePandasDataFrame,
    LocalDataFrameIterableDataFrame,
)
from .dataframes import DataFrames
from .iterable_dataframe import IterableDataFrame
from .pandas_dataframe import PandasDataFrame


def _compare_iter(tp: Any) -> Any:
    return lambda x: compare_annotations(
        x, Iterable[tp]  # type:ignore
    ) or compare_annotations(
        x, Iterator[tp]  # type:ignore
    )


@function_wrapper(FUGUE_ENTRYPOINT)
class DataFrameFunctionWrapper(FunctionWrapper):
    @property
    def need_output_schema(self) -> Optional[bool]:
        return (
            self._rt.need_schema()
            if isinstance(self._rt, _DataFrameParamBase)
            else False
        )

    def get_format_hint(self) -> Optional[str]:
        for v in self._params.values():
            if isinstance(v, _DataFrameParamBase):
                if v.format_hint() is not None:
                    return v.format_hint()
        if isinstance(self._rt, _DataFrameParamBase):
            return self._rt.format_hint()
        return None

    def run(  # noqa: C901
        self,
        args: List[Any],
        kwargs: Dict[str, Any],
        ignore_unknown: bool = False,
        output_schema: Any = None,
        output: bool = True,
        ctx: Any = None,
    ) -> Any:
        p: Dict[str, Any] = {}
        for i in range(len(args)):
            p[self._params.get_key_by_index(i)] = args[i]
        p.update(kwargs)
        has_kw = False
        rargs: Dict[str, Any] = {}
        row_param_info: Any = None
        for k, v in self._params.items():
            if isinstance(v, (PositionalParam, KeywordParam)):
                if isinstance(v, KeywordParam):
                    has_kw = True
            elif k in p:
                if isinstance(v, _DataFrameParamBase):
                    assert_or_throw(
                        isinstance(p[k], DataFrame),
                        lambda: TypeError(f"{p[k]} is not a DataFrame"),
                    )
                    if v.is_per_row:  # pragma: no cover
                        # TODO: this branch is used only if row annotations
                        # are allowed as input
                        assert_or_throw(
                            row_param_info is None,
                            lambda: ValueError("only one row parameter is allowed"),
                        )
                        row_param_info = (k, v, p[k])
                    else:
                        rargs[k] = v.to_input_data(p[k], ctx=ctx)
                else:
                    rargs[k] = p[k]  # TODO: should we do auto type conversion?
                del p[k]
            elif v.required:
                raise ValueError(f"{k} is required by not given")
        if has_kw:
            rargs.update(p)
        elif not ignore_unknown and len(p) > 0:
            raise ValueError(f"{p} are not acceptable parameters")
        if row_param_info is None:
            return self._run_func(rargs, output, output_schema, ctx, raw=False)
        else:  # pragma: no cover
            # input contains row parameter
            # TODO: this branch is used only if row annotations are allowed as input

            def _dfs() -> Iterable[Any]:
                k, v, df = row_param_info
                for row in v.to_input_rows(df, ctx):
                    rargs[k] = None
                    _rargs = rargs.copy()
                    _rargs[k] = row
                    yield self._run_func(_rargs, output, output_schema, ctx, raw=True)

            if not output:
                sum(1 for _ in _dfs())
                return
            else:
                return self._rt.iterable_to_output_df(_dfs(), output_schema, ctx)

    def _run_func(
        self,
        rargs: Dict[str, Any],
        output: bool,
        output_schema: Any,
        ctx: Any,
        raw: bool,
    ) -> Any:
        rt = self._func(**rargs)
        if not output:
            if isinstance(self._rt, _DataFrameParamBase):
                self._rt.count(rt)
            return
        if not raw and isinstance(self._rt, _DataFrameParamBase):
            return self._rt.to_output_df(rt, output_schema, ctx=ctx)
        return rt


fugue_annotated_param = DataFrameFunctionWrapper.annotated_param


@fugue_annotated_param(
    "Callable",
    "F",
    lambda annotation: (
        annotation == Callable
        or annotation == callable  # pylint: disable=comparison-with-callable
        or str(annotation).startswith("typing.Callable")
        or str(annotation).startswith("collections.abc.Callable")
    ),
)
class _CallableParam(AnnotatedParam):
    pass


@fugue_annotated_param(
    "Callable",
    "f",
    lambda annotation: (
        annotation == Optional[Callable]
        or annotation == Optional[callable]
        or str(annotation).startswith("typing.Union[typing.Callable")  # 3.8-
        or str(annotation).startswith("typing.Optional[typing.Callable")  # 3.9+
        or str(annotation).startswith(
            "typing.Optional[collections.abc.Callable]"
        )  # 3.9+
    ),
)
class _OptionalCallableParam(AnnotatedParam):
    pass


class _DataFrameParamBase(AnnotatedParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param)
        assert_or_throw(self.required, lambda: TypeError(f"{self} must be required"))

    @property
    def is_per_row(self) -> bool:
        return False

    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:  # pragma: no cover
        raise NotImplementedError

    def to_input_rows(
        self,
        df: DataFrame,
        ctx: Any,
    ) -> Iterable[Any]:
        raise NotImplementedError  # pragma: no cover

    def to_output_df(
        self, df: Any, schema: Any, ctx: Any
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    def iterable_to_output_df(
        self, dfs: Iterable[Any], schema: Any, ctx: Any
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    def count(self, df: Any) -> int:  # pragma: no cover
        raise NotImplementedError

    def need_schema(self) -> Optional[bool]:
        return False

    def format_hint(self) -> Optional[str]:
        return None


@fugue_annotated_param(DataFrame, "d", child_can_reuse_code=True)
class DataFrameParam(_DataFrameParamBase):
    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        return df

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert_or_throw(
            schema is None or output.schema == schema,
            lambda: f"Output schema mismatch {output.schema} vs {schema}",
        )
        return output

    def count(self, df: Any) -> int:
        if df.is_bounded:
            return df.count()
        else:
            return sum(1 for _ in df.as_array_iterable())


@fugue_annotated_param(DataFrame, "r", child_can_reuse_code=True)
class RowParam(_DataFrameParamBase):  # pragma: no cover
    # TODO: this class is used only if row annotations are allowed as input
    @property
    def is_per_row(self) -> bool:
        return True

    def count(self, df: Any) -> int:
        return 1


@fugue_annotated_param(Dict[str, Any])
class DictParam(RowParam):  # pragma: no cover
    # TODO: this class is used only if row annotations are allowed as input
    def to_input_rows(self, df: DataFrame, ctx: Any) -> Iterable[Any]:
        yield from df.as_dict_iterable()

    def to_output_df(self, output: Dict[str, Any], schema: Any, ctx: Any) -> DataFrame:
        return ArrayDataFrame([list(output.values())], schema)

    def iterable_to_output_df(
        self, dfs: Iterable[Dict[str, Any]], schema: Any, ctx: Any
    ) -> DataFrame:  # pragma: no cover
        params: Dict[str, Any] = {}
        if schema is not None:
            params["schema"] = Schema(schema).pa_schema
        adf = pa.Table.from_pylist(list(dfs), **params)
        return ArrowDataFrame(adf)


@fugue_annotated_param(AnyDataFrame)
class _AnyDataFrameParam(DataFrameParam):
    def to_output_df(self, output: AnyDataFrame, schema: Any, ctx: Any) -> DataFrame:
        return (
            as_fugue_df(output)
            if schema is None
            else as_fugue_df(output, schema=schema)
        )

    def count(self, df: Any) -> int:
        return df_count(df)


@fugue_annotated_param(LocalDataFrame, "l", child_can_reuse_code=True)
class LocalDataFrameParam(DataFrameParam):
    def to_input_data(self, df: DataFrame, ctx: Any) -> LocalDataFrame:
        return df.as_local()

    def to_output_df(self, output: LocalDataFrame, schema: Any, ctx: Any) -> DataFrame:
        assert_or_throw(
            schema is None or output.schema == schema,
            lambda: f"Output schema mismatch {output.schema} vs {schema}",
        )
        return output

    def iterable_to_output_df(
        self, dfs: Iterable[Any], schema: Any, ctx: Any
    ) -> DataFrame:  # pragma: no cover
        def _dfs() -> Iterable[DataFrame]:
            for df in dfs:
                yield self.to_output_df(df, schema, ctx)

        return LocalDataFrameIterableDataFrame(_dfs(), schema=schema)

    def count(self, df: LocalDataFrame) -> int:
        if df.is_bounded:
            return df.count()
        else:
            return sum(1 for _ in df.as_array_iterable())


@fugue_annotated_param(
    "[NoSchema]", "s", matcher=lambda x: False, child_can_reuse_code=True
)
class _LocalNoSchemaDataFrameParam(LocalDataFrameParam):
    def need_schema(self) -> Optional[bool]:
        return True


@fugue_annotated_param(List[List[Any]])
class _ListListParam(_LocalNoSchemaDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> List[List[Any]]:
        return df.as_array(type_safe=True)

    @no_type_check
    def to_output_df(self, output: List[List[Any]], schema: Any, ctx: Any) -> DataFrame:
        return ArrayDataFrame(output, schema)

    @no_type_check
    def count(self, df: List[List[Any]]) -> int:
        return len(df)


@fugue_annotated_param(Iterable[List[Any]], matcher=_compare_iter(List[Any]))
class _IterableListParam(_LocalNoSchemaDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> Iterable[List[Any]]:
        return df.as_array_iterable(type_safe=True)

    @no_type_check
    def to_output_df(
        self, output: Iterable[List[Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        return IterableDataFrame(output, schema)

    @no_type_check
    def count(self, df: Iterable[List[Any]]) -> int:
        return sum(1 for _ in df)


@fugue_annotated_param(EmptyAwareIterable[List[Any]])
class _EmptyAwareIterableListParam(_LocalNoSchemaDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> EmptyAwareIterable[List[Any]]:
        return make_empty_aware(df.as_array_iterable(type_safe=True))

    @no_type_check
    def to_output_df(
        self, output: EmptyAwareIterable[List[Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        return IterableDataFrame(output, schema)

    @no_type_check
    def count(self, df: EmptyAwareIterable[List[Any]]) -> int:
        return sum(1 for _ in df)


@fugue_annotated_param(List[Dict[str, Any]])
class _ListDictParam(_LocalNoSchemaDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> List[Dict[str, Any]]:
        return df.as_local().as_dicts()

    @no_type_check
    def to_output_df(
        self, output: List[Dict[str, Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        schema = schema if isinstance(schema, Schema) else Schema(schema)

        def get_all() -> Iterable[List[Any]]:
            for row in output:
                yield [row[x] for x in schema.names]

        return IterableDataFrame(get_all(), schema)

    @no_type_check
    def count(self, df: List[Dict[str, Any]]) -> int:
        return len(df)


@fugue_annotated_param(Iterable[Dict[str, Any]], matcher=_compare_iter(Dict[str, Any]))
class _IterableDictParam(_LocalNoSchemaDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> Iterable[Dict[str, Any]]:
        return df.as_dict_iterable()

    @no_type_check
    def to_output_df(
        self, output: Iterable[Dict[str, Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        schema = schema if isinstance(schema, Schema) else Schema(schema)

        def get_all() -> Iterable[List[Any]]:
            for row in output:
                yield [row[x] for x in schema.names]

        return IterableDataFrame(get_all(), schema)

    @no_type_check
    def count(self, df: Iterable[Dict[str, Any]]) -> int:
        return sum(1 for _ in df)


@fugue_annotated_param(EmptyAwareIterable[Dict[str, Any]])
class _EmptyAwareIterableDictParam(_LocalNoSchemaDataFrameParam):
    @no_type_check
    def to_input_data(
        self, df: DataFrame, ctx: Any
    ) -> EmptyAwareIterable[Dict[str, Any]]:
        return make_empty_aware(df.as_dict_iterable())

    @no_type_check
    def to_output_df(
        self, output: EmptyAwareIterable[Dict[str, Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        schema = schema if isinstance(schema, Schema) else Schema(schema)

        def get_all() -> Iterable[List[Any]]:
            for row in output:
                yield [row[x] for x in schema.names]

        return IterableDataFrame(get_all(), schema)

    @no_type_check
    def count(self, df: EmptyAwareIterable[Dict[str, Any]]) -> int:
        return sum(1 for _ in df)


@fugue_annotated_param(pd.DataFrame, "p")
class _PandasParam(LocalDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> pd.DataFrame:
        return df.as_pandas()

    @no_type_check
    def to_output_df(self, output: pd.DataFrame, schema: Any, ctx: Any) -> DataFrame:
        _schema: Optional[Schema] = None if schema is None else Schema(schema)
        if _schema is not None and _schema.names != list(output.columns):
            output = output[_schema.names]
        return PandasDataFrame(output, schema)

    @no_type_check
    def count(self, df: pd.DataFrame) -> int:
        return df.shape[0]

    def format_hint(self) -> Optional[str]:
        return "pandas"


@fugue_annotated_param(Iterable[pd.DataFrame], matcher=_compare_iter(pd.DataFrame))
class _IterablePandasParam(LocalDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> Iterable[pd.DataFrame]:
        if not isinstance(df, LocalDataFrameIterableDataFrame):
            yield df.as_pandas()
        else:
            for sub in df.native:
                yield sub.as_pandas()

    @no_type_check
    def to_output_df(
        self, output: Iterable[pd.DataFrame], schema: Any, ctx: Any
    ) -> DataFrame:
        def dfs():
            _schema: Optional[Schema] = None if schema is None else Schema(schema)
            has_return = False
            for df in output:
                if _schema is not None and _schema.names != list(df.columns):
                    df = df[_schema.names]
                yield PandasDataFrame(df, _schema)
                has_return = True
            if not has_return and _schema is not None:
                yield PandasDataFrame(schema=_schema)

        return IterablePandasDataFrame(dfs())

    @no_type_check
    def count(self, df: Iterable[pd.DataFrame]) -> int:
        return sum(_.shape[0] for _ in df)

    def format_hint(self) -> Optional[str]:
        return "pandas"


@fugue_annotated_param(pa.Table)
class _PyArrowTableParam(LocalDataFrameParam):
    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        return df.as_arrow()

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, pa.Table)
        adf: DataFrame = ArrowDataFrame(output)
        if schema is not None:
            _schema = Schema(schema)
            if adf.schema != _schema:
                adf = adf[_schema.names].alter_columns(_schema)
        return adf

    def count(self, df: Any) -> int:  # pragma: no cover
        return df.count()

    def format_hint(self) -> Optional[str]:
        return "pyarrow"


@fugue_annotated_param(Iterable[pa.Table], matcher=_compare_iter(pa.Table))
class _IterableArrowParam(LocalDataFrameParam):
    @no_type_check
    def to_input_data(self, df: DataFrame, ctx: Any) -> Iterable[pa.Table]:
        if not isinstance(df, LocalDataFrameIterableDataFrame):
            yield df.as_arrow()
        else:
            for sub in df.native:
                yield sub.as_arrow()

    @no_type_check
    def to_output_df(
        self, output: Iterable[pa.Table], schema: Any, ctx: Any
    ) -> DataFrame:
        def dfs():
            _schema: Optional[Schema] = None if schema is None else Schema(schema)
            has_return = False
            for df in output:
                adf: DataFrame = ArrowDataFrame(df)
                if _schema is not None and adf.schema != _schema:
                    adf = adf[_schema.names].alter_columns(_schema)
                yield adf
                has_return = True
            if not has_return and _schema is not None:
                yield ArrowDataFrame(schema=_schema)

        return IterableArrowDataFrame(dfs())

    @no_type_check
    def count(self, df: Iterable[pa.Table]) -> int:
        return sum(_.shape[0] for _ in df)

    def format_hint(self) -> Optional[str]:
        return "pyarrow"


@fugue_annotated_param(DataFrames, "c")
class _DataFramesParam(AnnotatedParam):
    pass
