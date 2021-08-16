import copy
import inspect
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    get_type_hints,
)

import pandas as pd
from fugue.dataframe import (
    ArrayDataFrame,
    DataFrame,
    IterableDataFrame,
    LocalDataFrame,
    LocalDataFrameIterableDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.dataframes import DataFrames
from fugue.dataframe.utils import to_local_df
from fugue.exceptions import FugueWorkflowRuntimeError
from triad import IndexedOrderedDict
from triad.collections import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import get_full_type_path, to_type
from triad.utils.hash import to_uuid
from triad.utils.iter import EmptyAwareIterable, make_empty_aware

_COMMENT_SCHEMA_ANNOTATION = "schema"


def parse_comment_annotation(func: Callable, annotation: str) -> Optional[str]:
    """Parse comment annotation above the function. It try to find
    comment lines starts with the annotation from bottom up, and will use the first
    occurrance as the result.

    :param func: the function
    :param annotation: the annotation string
    :return: schema hint string

    .. admonition:: Examples

        .. code-block:: python

            # schema: a:int,b:str
            #schema:a:int,b:int # more comment
            # some comment
            def dummy():
                pass

            assert "a:int,b:int" == parse_comment_annotation(dummy, "schema:")
    """
    for orig in reversed((inspect.getcomments(func) or "").splitlines()):
        start = orig.find(":")
        if start <= 0:
            continue
        actual = orig[:start].replace("#", "", 1).strip()
        if actual != annotation:
            continue
        end = orig.find("#", start)
        s = orig[start + 1 : (end if end > 0 else len(orig))].strip()
        return s
    return None


def parse_output_schema_from_comment(func: Callable) -> Optional[str]:
    """Parse schema hint from the comments above the function. It try to find
    comment lines starts with `schema:` from bottom up, and will use the first
    occurrance as the hint.

    :param func: the function
    :return: schema hint string

    .. admonition:: Examples

        .. code-block:: python

            # schema: a:int,b:str
            #schema:a:int,b:int # more comment
            # some comment
            def dummy():
                pass

            assert "a:int,b:int" == parse_output_schema_from_comment(dummy)
    """
    res = parse_comment_annotation(func, _COMMENT_SCHEMA_ANNOTATION)
    if res is None:
        return None
    assert_or_throw(res != "", SyntaxError("incorrect schema annotation"))
    return res.replace(" ", "")


def is_class_method(func: Callable) -> bool:
    sig = inspect.signature(func)
    # TODO: this is not the best way
    return "self" in sig.parameters


class AnnotationConverter:
    def check(self, annotation: Any) -> bool:  # pragma: no cover
        raise NotImplementedError

    def convert(
        self, param: Optional[inspect.Parameter]
    ) -> "_FuncParam":  # pragma: no cover
        raise NotImplementedError


class SimpleAnnotationConverter(AnnotationConverter):
    def __init__(
        self,
        expected_annotation,
        converter: Callable[[Optional[inspect.Parameter]], "_FuncParam"],
    ) -> None:
        self._expected = expected_annotation
        self._converter = converter

    def check(self, annotation: Any) -> bool:
        return annotation == self._expected

    def convert(self, param: Optional[inspect.Parameter]) -> "_FuncParam":
        return self._converter(param)


_ANNOTATION_CONVERTERS: List[Tuple[float, AnnotationConverter]] = []


def register_annotation_converter(
    priority: float, converter: AnnotationConverter
) -> None:
    """Register a new annotation for Fugue's interfaceless system

    :param priority: priority number, smaller means higher priority for checking
    :param converter: a new converter

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. note::

        This is not ready for public use yet, the interface is subjected to change

    """
    _ANNOTATION_CONVERTERS.append((priority, converter))
    _ANNOTATION_CONVERTERS.sort(key=lambda x: x[0])


class FunctionWrapper(object):
    def __init__(
        self,
        func: Callable,
        params_re: str = ".*",
        return_re: str = ".*",
    ):
        self._class_method, self._params, self._rt = self._parse_function(
            func, params_re, return_re
        )
        self._func = func

    def __deepcopy__(self, memo: Any) -> Any:
        return copy.copy(self)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._func(*args, **kwargs)

    def __uuid__(self) -> str:
        return to_uuid(get_full_type_path(self._func), self._params, self._rt)

    @property
    def input_code(self) -> str:
        return "".join(x.code for x in self._params.values())

    @property
    def need_output_schema(self) -> Optional[bool]:
        return (
            self._rt.need_schema()
            if isinstance(self._rt, _DataFrameParamBase)
            else False
        )

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
        for k, v in self._params.items():
            if isinstance(v, (_PositionalParam, _KeywordParam)):
                if isinstance(v, _KeywordParam):
                    has_kw = True
            elif k in p:
                if isinstance(v, _DataFrameParamBase):
                    assert_or_throw(
                        isinstance(p[k], DataFrame),
                        lambda: TypeError(f"{p[k]} is not a DataFrame"),
                    )
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
        rt = self._func(**rargs)
        if not output:
            if isinstance(self._rt, _DataFrameParamBase):
                self._rt.count(rt)
            return
        if isinstance(self._rt, _DataFrameParamBase):
            return self._rt.to_output_df(rt, output_schema, ctx=ctx)
        return rt

    def _parse_function(
        self, func: Callable, params_re: str = ".*", return_re: str = ".*"
    ) -> Tuple[bool, IndexedOrderedDict[str, "_FuncParam"], "_FuncParam"]:
        sig = inspect.signature(func)
        annotations = get_type_hints(func)
        res: IndexedOrderedDict[str, "_FuncParam"] = IndexedOrderedDict()
        class_method = False
        for k, w in sig.parameters.items():
            if k == "self":
                res[k] = _SelfParam(w)
                class_method = True
            else:
                anno = annotations.get(k, w.annotation)
                res[k] = self._parse_param(anno, w)
        anno = annotations.get("return", sig.return_annotation)
        rt = self._parse_param(anno, None, none_as_other=False)
        params_str = "".join(x.code for x in res.values())
        assert_or_throw(
            re.match(params_re, params_str),
            lambda: TypeError(f"Input types not valid {res} for {func}"),
        )
        assert_or_throw(
            re.match(return_re, rt.code),
            lambda: TypeError(f"Return type not valid {rt} for {func}"),
        )
        return class_method, res, rt

    def _parse_param(  # noqa: C901
        self,
        annotation: Any,
        param: Optional[inspect.Parameter],
        none_as_other: bool = True,
    ) -> "_FuncParam":
        if annotation == type(None):  # noqa: E721
            return _NoneParam(param)
        if annotation == inspect.Parameter.empty:
            if param is not None and param.kind == param.VAR_POSITIONAL:
                return _PositionalParam(param)
            if param is not None and param.kind == param.VAR_KEYWORD:
                return _KeywordParam(param)
            return _OtherParam(param) if none_as_other else _NoneParam(param)
        if (
            annotation == Callable
            or annotation == callable  # pylint: disable=comparison-with-callable
            or str(annotation).startswith("typing.Callable")
        ):
            return _CallableParam(param)
        if (
            annotation == Optional[Callable]
            or annotation == Optional[callable]
            or str(annotation).startswith("typing.Union[typing.Callable")
        ):
            return _OptionalCallableParam(param)
        for _, c in _ANNOTATION_CONVERTERS:
            if c.check(annotation):
                return c.convert(param)
        if annotation == to_type("fugue.execution.ExecutionEngine"):
            # to prevent cyclic import
            return ExecutionEngineParam(param, "ExecutionEngine", annotation)
        if annotation == DataFrames:
            return _DataFramesParam(param)
        if annotation == LocalDataFrame:
            return _LocalDataFrameParam(param)
        if annotation == DataFrame:
            return DataFrameParam(param)
        if annotation == pd.DataFrame:
            return _PandasParam(param)
        if annotation == List[List[Any]]:
            return _ListListParam(param)
        if annotation == Iterable[List[Any]]:
            return _IterableListParam(param)
        if annotation == EmptyAwareIterable[List[Any]]:
            return _EmptyAwareIterableListParam(param)
        if annotation == List[Dict[str, Any]]:
            return _ListDictParam(param)
        if annotation == Iterable[Dict[str, Any]]:
            return _IterableDictParam(param)
        if annotation == EmptyAwareIterable[Dict[str, Any]]:
            return _EmptyAwareIterableDictParam(param)
        if annotation == Iterable[pd.DataFrame]:
            return _IterablePandasParam(param)
        if param is not None and param.kind == param.VAR_POSITIONAL:
            return _PositionalParam(param)
        if param is not None and param.kind == param.VAR_KEYWORD:
            return _KeywordParam(param)
        return _OtherParam(param)


class _FuncParam(object):
    def __init__(self, param: Optional[inspect.Parameter], annotation: Any, code: str):
        if param is not None:
            self.required = param.default == inspect.Parameter.empty
            self.default = param.default
        else:
            self.required, self.default = True, None
        self.code = code
        self.annotation = annotation

    def __repr__(self) -> str:
        return str(self.annotation)


class _CallableParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "Callable", "F")


class _OptionalCallableParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "Callable", "f")


class ExecutionEngineParam(_FuncParam):
    def __init__(
        self,
        param: Optional[inspect.Parameter],
        annotation: str,
        engine_type: Type,
    ):
        super().__init__(param, annotation, "e")
        self._type = engine_type

    def to_input(self, engine: Any) -> Any:  # pragma: no cover
        assert_or_throw(
            isinstance(engine, self._type),
            FugueWorkflowRuntimeError(f"{engine} is not of type {self._type}"),
        )
        return engine

    def __uuid__(self) -> str:
        return to_uuid(self.code, self.annotation, self._type)


class _DataFramesParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "DataFrames", "c")


class _DataFrameParamBase(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter], annotation: Any, code: str):
        super().__init__(param, annotation, code)
        assert_or_throw(self.required, lambda: TypeError(f"{self} must be required"))

    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:  # pragma: no cover
        raise NotImplementedError

    def to_output_df(
        self, df: Any, schema: Any, ctx: Any
    ) -> DataFrame:  # pragma: no cover
        raise NotImplementedError

    def count(self, df: Any) -> int:  # pragma: no cover
        raise NotImplementedError

    def need_schema(self) -> Optional[bool]:
        return False


class DataFrameParam(_DataFrameParamBase):
    def __init__(
        self, param: Optional[inspect.Parameter], annotation: str = "DataFrame"
    ):
        super().__init__(param, annotation=annotation, code="d")

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


class _LocalDataFrameParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "LocalDataFrame", "l")

    def to_input_data(self, df: DataFrame, ctx: Any) -> LocalDataFrame:
        return to_local_df(df)

    def to_output_df(self, output: LocalDataFrame, schema: Any, ctx: Any) -> DataFrame:
        assert_or_throw(
            schema is None or output.schema == schema,
            lambda: f"Output schema mismatch {output.schema} vs {schema}",
        )
        return output

    def count(self, df: LocalDataFrame) -> int:
        if df.is_bounded:
            return df.count()
        else:
            return sum(1 for _ in df.as_array_iterable())


class _ListListParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "List[List[Any]]", "s")

    def to_input_data(self, df: DataFrame, ctx: Any) -> List[List[Any]]:
        return df.as_array(type_safe=True)

    def to_output_df(self, output: List[List[Any]], schema: Any, ctx: Any) -> DataFrame:
        return ArrayDataFrame(output, schema)

    def count(self, df: List[List[Any]]) -> int:
        return len(df)

    def need_schema(self) -> Optional[bool]:
        return True


class _IterableListParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "Iterable[List[Any]]", "s")

    def to_input_data(self, df: DataFrame, ctx: Any) -> Iterable[List[Any]]:
        return df.as_array_iterable(type_safe=True)

    def to_output_df(
        self, output: Iterable[List[Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        return IterableDataFrame(output, schema)

    def count(self, df: Iterable[List[Any]]) -> int:
        return sum(1 for _ in df)

    def need_schema(self) -> Optional[bool]:  # pragma: no cover
        return True


class _EmptyAwareIterableListParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "EmptyAwareIterable[List[Any]]", "s")

    def to_input_data(self, df: DataFrame, ctx: Any) -> EmptyAwareIterable[List[Any]]:
        return make_empty_aware(df.as_array_iterable(type_safe=True))

    def to_output_df(
        self, output: EmptyAwareIterable[List[Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        return IterableDataFrame(output, schema)

    def count(self, df: EmptyAwareIterable[List[Any]]) -> int:
        return sum(1 for _ in df)

    def need_schema(self) -> Optional[bool]:  # pragma: no cover
        return True


class _ListDictParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "List[Dict[str,Any]]", "s")

    def to_input_data(self, df: DataFrame, ctx: Any) -> List[Dict[str, Any]]:
        return list(to_local_df(df).as_dict_iterable())

    def to_output_df(
        self, output: List[Dict[str, Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        schema = schema if isinstance(schema, Schema) else Schema(schema)

        def get_all() -> Iterable[List[Any]]:
            for row in output:
                yield [row[x] for x in schema.names]

        return IterableDataFrame(get_all(), schema)

    def count(self, df: List[Dict[str, Any]]) -> int:
        return len(df)

    def need_schema(self) -> Optional[bool]:  # pragma: no cover
        return True


class _IterableDictParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "Iterable[Dict[str,Any]]", "s")

    def to_input_data(self, df: DataFrame, ctx: Any) -> Iterable[Dict[str, Any]]:
        return df.as_dict_iterable()

    def to_output_df(
        self, output: Iterable[Dict[str, Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        schema = schema if isinstance(schema, Schema) else Schema(schema)

        def get_all() -> Iterable[List[Any]]:
            for row in output:
                yield [row[x] for x in schema.names]

        return IterableDataFrame(get_all(), schema)

    def count(self, df: Iterable[Dict[str, Any]]) -> int:
        return sum(1 for _ in df)

    def need_schema(self) -> Optional[bool]:  # pragma: no cover
        return True


class _EmptyAwareIterableDictParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "EmptyAwareIterable[Dict[str,Any]]", "s")

    def to_input_data(
        self, df: DataFrame, ctx: Any
    ) -> EmptyAwareIterable[Dict[str, Any]]:
        return make_empty_aware(df.as_dict_iterable())

    def to_output_df(
        self, output: EmptyAwareIterable[Dict[str, Any]], schema: Any, ctx: Any
    ) -> DataFrame:
        schema = schema if isinstance(schema, Schema) else Schema(schema)

        def get_all() -> Iterable[List[Any]]:
            for row in output:
                yield [row[x] for x in schema.names]

        return IterableDataFrame(get_all(), schema)

    def count(self, df: EmptyAwareIterable[Dict[str, Any]]) -> int:
        return sum(1 for _ in df)

    def need_schema(self) -> Optional[bool]:  # pragma: no cover
        return True


class _PandasParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "pd.DataFrame", "p")

    def to_input_data(self, df: DataFrame, ctx: Any) -> pd.DataFrame:
        return df.as_pandas()

    def to_output_df(self, output: pd.DataFrame, schema: Any, ctx: Any) -> DataFrame:
        return PandasDataFrame(output, schema)

    def count(self, df: pd.DataFrame) -> int:
        return df.shape[0]


class _IterablePandasParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "Iterable[pd.DataFrame]", "q")

    def to_input_data(self, df: DataFrame, ctx: Any) -> Iterable[pd.DataFrame]:
        if not isinstance(df, LocalDataFrameIterableDataFrame):
            yield df.as_pandas()
        else:
            for sub in df.native:
                yield sub.as_pandas()

    def to_output_df(
        self, output: Iterable[pd.DataFrame], schema: Any, ctx: Any
    ) -> DataFrame:
        def dfs():
            for df in output:
                yield PandasDataFrame(df, schema)

        return LocalDataFrameIterableDataFrame(dfs())

    def count(self, df: Iterable[pd.DataFrame]) -> int:
        return sum(_.shape[0] for _ in df)


class _NoneParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "NoneType", "n")


class _SelfParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "[Self]", "0")


class _OtherParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "[Other]", "x")


class _PositionalParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "[Positional]", "y")


class _KeywordParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "[Keyword]", "z")
