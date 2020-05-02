import copy
from typing import Any, Callable, List, Optional, Union

from fugue.dataframe import DataFrame, LocalDataFrame
from fugue.exceptions import FugueInterfacelessError
from fugue.transformer.transformer import MultiInputTransformer, Transformer
from fugue.utils.interfaceless import FunctionWrapper
from triad.collections.schema import Schema
from triad.utils.assertion import assert_arg_not_none
from triad.utils.convert import to_function, to_instance


class _FuncAsTransformer(Transformer):
    def get_output_schema(self, df: DataFrame) -> Any:
        return self._parse_schema(self._output_schema_arg, df)  # type: ignore

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        return self._wrapper.run(  # type: ignore
            [df], self.params, ignore_unknown=False, output_schema=self.output_schema
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrapper(*args, **kwargs)  # type: ignore

    def _parse_schema(self, obj: Any, df: DataFrame) -> Schema:
        if obj is None:
            return Schema()
        if isinstance(obj, str):
            if "*" in obj:
                obj = obj.replace("*", str(df.schema))
            return Schema(obj)
        if isinstance(obj, List):
            s = Schema()
            for x in obj:
                s += self._parse_schema(x, df)
            return s
        return Schema(obj)

    @staticmethod
    def from_func(func: Callable, schema: Any) -> "_FuncAsTransformer":
        assert_arg_not_none(schema, "schema")
        tr = _FuncAsTransformer()
        tr._wrapper = FunctionWrapper(func, "^[lsp]x*$", "^[lsp]$")  # type: ignore
        tr._output_schema_arg = schema  # type: ignore
        return tr


def transformer(schema: Any) -> Callable[[Any], _FuncAsTransformer]:
    def deco(func: Callable) -> _FuncAsTransformer:
        return _FuncAsTransformer.from_func(func, schema)

    return deco


def to_transformer(obj: Any, schema: Any) -> Union[Transformer, MultiInputTransformer]:
    exp: Optional[Exception] = None
    try:
        return copy.copy(to_instance(obj, Transformer))
    except Exception as e:
        exp = e
    try:
        return copy.copy(to_instance(obj, MultiInputTransformer))
    except Exception as e:
        exp = e
    try:
        f = to_function(obj)
        # this is for string expression of function with decorator
        if isinstance(f, (Transformer, MultiInputTransformer)):
            return copy.copy(f)
        # this is for functions without decorator
        return _FuncAsTransformer.from_func(f, schema)
    except Exception as e:
        exp = e
    raise FugueInterfacelessError(f"{obj} is not a valid transformer", exp)
