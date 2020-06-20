import inspect
import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, get_type_hints

import pandas as pd
from fugue.collections.partition import IndexedOrderedDict
from fugue.dataframe import (
    ArrayDataFrame,
    DataFrame,
    IterableDataFrame,
    LocalDataFrame,
    PandasDataFrame,
)
from fugue.dataframe.dataframes import DataFrames
from fugue.dataframe.utils import to_local_df
from triad.collections import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import get_full_type_path, to_type
from triad.utils.iter import EmptyAwareIterable, make_empty_aware
from triad.utils.hash import to_uuid

_COMMENT_SCHEMA_ANNOTATION = "schema:"


def parse_output_schema_from_comment(func: Callable) -> Optional[str]:
    """Parse schema hint from the comments above the function. It try to find
    comment lines starts with `schema:` from bottom up, and will use the first
    occurrance as the hint.

    :param func: the function
    :return: schema hint string

    :Example:

    .. code-block:: python

        # schema: a:int,b:str
        #schema:a:int,b:int
        # some comment
        def dummy():
            pass

        assert "a:int,b:int" == parse_output_schema_from_comment(dummy)
    """
    for comment in reversed((inspect.getcomments(func) or "").splitlines()):
        comment = comment.replace(" ", "").replace("#", "")
        if not comment.startswith(_COMMENT_SCHEMA_ANNOTATION):
            continue
        s = comment[len(_COMMENT_SCHEMA_ANNOTATION) :]
        if s != "":
            return s
    return None


class FunctionWrapper(object):
    def __init__(self, func: Callable, params_re: str = ".*", return_re: str = ".*"):
        self._params, self._rt = _parse_function(func, params_re, return_re)
        self._func = func

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._func(*args, **kwargs)

    def __uuid__(self) -> str:
        return to_uuid(get_full_type_path(self._func), self._params, self._rt)

    @property
    def input_code(self) -> str:
        return "".join(x.code for x in self._params.values())

    @property
    def output_code(self) -> str:
        return self._rt.code

    def run(  # noqa: C901
        self,
        args: List[Any],
        kwargs: Dict[str, Any],
        ignore_unknown: bool = False,
        output_schema: Any = None,
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
                        TypeError(f"{p[k]} is not a DataFrame"),
                    )
                    rargs[k] = v.to_input_data(p[k])
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
        if isinstance(self._rt, _DataFrameParamBase):
            return self._rt.to_output_df(rt, output_schema)
        return rt


def _parse_function(
    func: Callable, params_re: str = ".*", return_re: str = ".*"
) -> Tuple[IndexedOrderedDict[str, "_FuncParam"], "_FuncParam"]:
    sig = inspect.signature(func)
    annotations = get_type_hints(func)
    res: IndexedOrderedDict[str, "_FuncParam"] = IndexedOrderedDict()
    for k, w in sig.parameters.items():
        anno = annotations.get(k, w.annotation)
        res[k] = _parse_param(anno, w)
    anno = annotations.get("return", sig.return_annotation)
    rt = _parse_param(anno, None, none_as_other=False)
    params_str = "".join(x.code for x in res.values())
    assert_or_throw(
        re.match(params_re, params_str), TypeError(f"Input types not valid {res}")
    )
    assert_or_throw(
        re.match(return_re, rt.code), TypeError(f"Return type not valid {rt}")
    )
    return res, rt


def _parse_param(  # noqa: C901
    annotation: Any, param: Optional[inspect.Parameter], none_as_other: bool = True
) -> "_FuncParam":
    if annotation is type(None):  # noqa: E721
        return _NoneParam(param)
    if annotation == inspect.Parameter.empty:
        if param is not None and param.kind == param.VAR_POSITIONAL:
            return _PositionalParam(param)
        if param is not None and param.kind == param.VAR_KEYWORD:
            return _KeywordParam(param)
        return _OtherParam(param) if none_as_other else _NoneParam(param)
    if annotation is to_type("fugue.execution.ExecutionEngine"):
        # to prevent cyclic import
        return _ExecutionEngineParam(param)
    if annotation is DataFrames:
        return _DataFramesParam(param)
    if annotation is LocalDataFrame:
        return _LocalDataFrameParam(param)
    if annotation is DataFrame:
        return _DataFrameParam(param)
    if annotation is pd.DataFrame:
        return _PandasParam(param)
    if annotation is List[List[Any]]:
        return _ListListParam(param)
    if annotation is Iterable[List[Any]]:
        return _IterableListParam(param)
    if annotation is EmptyAwareIterable[List[Any]]:
        return _EmptyAwareIterableListParam(param)
    if annotation is List[Dict[str, Any]]:
        return _ListDictParam(param)
    if annotation is Iterable[Dict[str, Any]]:
        return _IterableDictParam(param)
    if annotation is EmptyAwareIterable[Dict[str, Any]]:
        return _EmptyAwareIterableDictParam(param)
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


class _ExecutionEngineParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "ExecutionEngine", "e")


class _DataFramesParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "DataFrames", "c")


class _DataFrameParamBase(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter], annotation: Any, code: str):
        super().__init__(param, annotation, code)
        assert_or_throw(self.required, TypeError(f"{self} must be required"))

    def to_input_data(self, df: DataFrame) -> Any:  # pragma: no cover
        raise NotImplementedError

    def to_output_df(self, df: Any, schema: Any) -> DataFrame:  # pragma: no cover
        raise NotImplementedError


class _DataFrameParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "DataFrame", "d")

    def to_input_data(self, df: DataFrame) -> Any:
        return df

    def to_output_df(self, output: DataFrame, schema: Any) -> DataFrame:
        assert_or_throw(
            schema is None or output.schema == schema,
            f"Output schema mismatch {output.schema} vs {schema}",
        )
        return output


class _LocalDataFrameParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "LocalDataFrame", "l")

    def to_input_data(self, df: DataFrame) -> LocalDataFrame:
        return to_local_df(df)

    def to_output_df(self, output: LocalDataFrame, schema: Any) -> DataFrame:
        assert_or_throw(
            schema is None or output.schema == schema,
            f"Output schema mismatch {output.schema} vs {schema}",
        )
        return output


class _ListListParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "List[List[Any]]", "s")

    def to_input_data(self, df: DataFrame) -> List[List[Any]]:
        return df.as_array(type_safe=True)

    def to_output_df(self, output: List[List[Any]], schema: Any) -> DataFrame:
        return ArrayDataFrame(output, schema)


class _IterableListParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "Iterable[List[Any]]", "s")

    def to_input_data(self, df: DataFrame) -> Iterable[List[Any]]:
        return df.as_array_iterable(type_safe=True)

    def to_output_df(self, output: Iterable[List[Any]], schema: Any) -> DataFrame:
        return IterableDataFrame(output, schema)


class _EmptyAwareIterableListParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "EmptyAwareIterable[List[Any]]", "s")

    def to_input_data(self, df: DataFrame) -> EmptyAwareIterable[List[Any]]:
        return make_empty_aware(df.as_array_iterable(type_safe=True))

    def to_output_df(
        self, output: EmptyAwareIterable[List[Any]], schema: Any
    ) -> DataFrame:
        return IterableDataFrame(output, schema)


class _ListDictParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "List[Dict[str,Any]]", "s")

    def to_input_data(self, df: DataFrame) -> List[Dict[str, Any]]:
        return list(to_local_df(df).as_dict_iterable())

    def to_output_df(self, output: List[Dict[str, Any]], schema: Any) -> DataFrame:
        schema = schema if isinstance(schema, Schema) else Schema(schema)

        def get_all() -> Iterable[List[Any]]:
            for row in output:
                yield [row[x] for x in schema.names]

        return IterableDataFrame(get_all(), schema)


class _IterableDictParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "Iterable[Dict[str,Any]]", "s")

    def to_input_data(self, df: DataFrame) -> Iterable[Dict[str, Any]]:
        return df.as_dict_iterable()

    def to_output_df(self, output: Iterable[Dict[str, Any]], schema: Any) -> DataFrame:
        schema = schema if isinstance(schema, Schema) else Schema(schema)

        def get_all() -> Iterable[List[Any]]:
            for row in output:
                yield [row[x] for x in schema.names]

        return IterableDataFrame(get_all(), schema)


class _EmptyAwareIterableDictParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "EmptyAwareIterable[Dict[str,Any]]", "s")

    def to_input_data(self, df: DataFrame) -> EmptyAwareIterable[Dict[str, Any]]:
        return make_empty_aware(df.as_dict_iterable())

    def to_output_df(
        self, output: EmptyAwareIterable[Dict[str, Any]], schema: Any
    ) -> DataFrame:
        schema = schema if isinstance(schema, Schema) else Schema(schema)

        def get_all() -> Iterable[List[Any]]:
            for row in output:
                yield [row[x] for x in schema.names]

        return IterableDataFrame(get_all(), schema)


class _PandasParam(_DataFrameParamBase):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "pd.DataFrame", "p")

    def to_input_data(self, df: DataFrame) -> pd.DataFrame:
        return df.as_pandas()

    def to_output_df(self, output: pd.DataFrame, schema: Any) -> DataFrame:
        return PandasDataFrame(output, schema)


class _NoneParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "NoneType", "n")


class _OtherParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "[Other]", "x")


class _PositionalParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "[Positional]", "y")


class _KeywordParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "[Keyword]", "z")
