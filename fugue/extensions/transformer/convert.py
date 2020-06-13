import copy
from typing import Any, Callable, List, Optional, Union, no_type_check

from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame
from fugue.exceptions import FugueInterfacelessError
from fugue.extensions.transformer.transformer import CoTransformer, Transformer
from fugue.utils.interfaceless import FunctionWrapper, parse_output_schema_from_comment
from triad.collections.schema import Schema
from triad.utils.assertion import assert_arg_not_none
from triad.utils.convert import to_function, to_instance
from triad.utils.hash import to_uuid


def transformer(schema: Any) -> Callable[[Any], "_FuncAsTransformer"]:
    # TODO: validation of schema if without * should be done at compile time
    def deco(func: Callable) -> _FuncAsTransformer:
        return _FuncAsTransformer.from_func(func, schema)

    return deco


def cotransformer(schema: Any) -> Callable[[Any], "_FuncAsCoTransformer"]:
    # TODO: validation of schema if without * should be done at compile time
    def deco(func: Callable) -> _FuncAsCoTransformer:
        return _FuncAsCoTransformer.from_func(func, schema)

    return deco


def to_transformer(  # noqa: C901
    obj: Any, schema: Any = None
) -> Union[Transformer, CoTransformer]:
    exp: Optional[Exception] = None
    try:
        return copy.copy(to_instance(obj, Transformer))
    except Exception as e:
        exp = e
    try:
        return copy.copy(to_instance(obj, CoTransformer))
    except Exception as e:
        exp = e
    try:
        f = to_function(obj)
        # this is for string expression of function with decorator
        if isinstance(f, Transformer):
            return copy.copy(f)
        # this is for functions without decorator
        return _FuncAsTransformer.from_func(f, schema)
    except Exception as e:
        exp = e
    try:
        f = to_function(obj)
        # this is for string expression of function with decorator
        if isinstance(f, CoTransformer):
            return copy.copy(f)
        # this is for functions without decorator
        return _FuncAsCoTransformer.from_func(f, schema)
    except Exception as e:
        exp = e
    raise FugueInterfacelessError(f"{obj} is not a valid transformer", exp)


class _FuncAsTransformer(Transformer):
    def get_output_schema(self, df: DataFrame) -> Any:
        return self._parse_schema(self._output_schema_arg, df)  # type: ignore

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        return self._wrapper.run(  # type: ignore
            [df], self.params, ignore_unknown=False, output_schema=self.output_schema
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrapper(*args, **kwargs)  # type: ignore

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(self._wrapper.__uuid__(), self._output_schema_arg)

    def _parse_schema(self, obj: Any, df: DataFrame) -> Schema:
        if isinstance(obj, str):
            return df.schema.transform(obj)
        if isinstance(obj, List):
            return df.schema.transform(*obj)
        return df.schema.transform(obj)

    @staticmethod
    def from_func(func: Callable, schema: Any) -> "_FuncAsTransformer":
        if schema is None:
            schema = parse_output_schema_from_comment(func)
        if isinstance(schema, Schema):  # to be less strict on determinism
            schema = str(schema)
        assert_arg_not_none(schema, "schema")
        tr = _FuncAsTransformer()
        tr._wrapper = FunctionWrapper(func, "^[lsp]x*$", "^[lsp]$")  # type: ignore
        tr._output_schema_arg = schema  # type: ignore
        return tr


class _FuncAsCoTransformer(CoTransformer):
    def get_output_schema(self, dfs: DataFrames) -> Any:
        return self._parse_schema(self._output_schema_arg)  # type: ignore

    @no_type_check
    def transform(self, dfs: DataFrames) -> LocalDataFrame:
        args: List[Any] = [dfs] if self._dfs_input else list(dfs.values())
        return self._wrapper.run(  # type: ignore
            args, self.params, ignore_unknown=False, output_schema=self.output_schema
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrapper(*args, **kwargs)  # type: ignore

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(
            self._wrapper.__uuid__(), self._output_schema_arg, self._dfs_input
        )

    def _parse_schema(self, obj: Any) -> Schema:
        if obj is None:
            return Schema()
        if isinstance(obj, str):
            return Schema(obj)
        if isinstance(obj, List):
            s = Schema()
            for x in obj:
                s += self._parse_schema(x)
            return s
        return Schema(obj)

    @staticmethod
    def from_func(func: Callable, schema: Any) -> "_FuncAsCoTransformer":
        if schema is None:
            schema = parse_output_schema_from_comment(func)
        if isinstance(schema, Schema):  # to be less strict on determinism
            schema = str(schema)
        assert_arg_not_none(schema, "schema")
        tr = _FuncAsCoTransformer()
        tr._wrapper = FunctionWrapper(func, "^(c|[lsp]+)x*$", "^[lsp]$")  # type: ignore
        tr._dfs_input = tr._wrapper.input_code[0] == "c"  # type: ignore
        tr._output_schema_arg = schema  # type: ignore
        return tr
