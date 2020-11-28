import copy
from typing import Any, Callable, Dict, List, Optional, Type, Union, no_type_check

from fugue._utils.interfaceless import (
    FunctionWrapper,
    is_class_method,
    parse_output_schema_from_comment,
)
from fugue.dataframe import ArrayDataFrame, DataFrame, DataFrames, LocalDataFrame
from fugue.exceptions import FugueInterfacelessError
from fugue.extensions._utils import (
    parse_validation_rules_from_comment,
    to_validation_rules,
)
from fugue.extensions.transformer.constants import OUTPUT_TRANSFORMER_DUMMY_SCHEMA
from fugue.extensions.transformer.transformer import CoTransformer, Transformer
from triad import ParamDict, Schema
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from triad.utils.convert import get_caller_global_local_vars, to_function, to_instance
from triad.utils.hash import to_uuid


def transformer(
    schema: Any, **validation_rules: Any
) -> Callable[[Any], "_FuncAsTransformer"]:
    """Decorator for transformers

    Please read :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`
    """

    def deco(func: Callable) -> "_FuncAsTransformer":
        assert_or_throw(
            not is_class_method(func),
            NotImplementedError("transformer decorator can't be used on class methods"),
        )
        return _FuncAsTransformer.from_func(
            func, schema, validation_rules=to_validation_rules(validation_rules)
        )

    return deco


def output_transformer(
    **validation_rules: Any,
) -> Callable[[Any], "_FuncAsTransformer"]:
    """Decorator for transformers

    Please read :ref:`Transformer Tutorial <tutorial:/tutorials/transformer.ipynb>`
    """

    def deco(func: Callable) -> "_FuncAsOutputTransformer":
        assert_or_throw(
            not is_class_method(func),
            NotImplementedError(
                "output_transformer decorator can't be used on class methods"
            ),
        )
        return _FuncAsOutputTransformer.from_func(
            func, schema=None, validation_rules=to_validation_rules(validation_rules)
        )

    return deco


def cotransformer(
    schema: Any, **validation_rules: Any
) -> Callable[[Any], "_FuncAsCoTransformer"]:
    """Decorator for cotransformers

    Please read :ref:`CoTransformer Tutorial <tutorial:/tutorials/cotransformer.ipynb>`
    """

    def deco(func: Callable) -> "_FuncAsCoTransformer":
        assert_or_throw(
            not is_class_method(func),
            NotImplementedError(
                "cotransformer decorator can't be used on class methods"
            ),
        )
        return _FuncAsCoTransformer.from_func(
            func, schema, validation_rules=to_validation_rules(validation_rules)
        )

    return deco


def output_cotransformer(
    **validation_rules: Any,
) -> Callable[[Any], "_FuncAsCoTransformer"]:
    """Decorator for cotransformers

    Please read :ref:`CoTransformer Tutorial <tutorial:/tutorials/cotransformer.ipynb>`
    """

    def deco(func: Callable) -> "_FuncAsOutputCoTransformer":
        assert_or_throw(
            not is_class_method(func),
            NotImplementedError(
                "output_cotransformer decorator can't be used on class methods"
            ),
        )
        return _FuncAsOutputCoTransformer.from_func(
            func, schema=None, validation_rules=to_validation_rules(validation_rules)
        )

    return deco


class _FuncAsTransformer(Transformer):
    def get_output_schema(self, df: DataFrame) -> Any:
        return self._parse_schema(self._output_schema_arg, df)  # type: ignore

    @property
    def validation_rules(self) -> Dict[str, Any]:
        return self._validation_rules  # type: ignore

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
        if callable(obj):
            return obj(df, **self.params)
        if isinstance(obj, str):
            return df.schema.transform(obj)
        if isinstance(obj, List):
            return df.schema.transform(*obj)
        raise NotImplementedError  # pragma: no cover

    @staticmethod
    def from_func(
        func: Callable, schema: Any, validation_rules: Dict[str, Any]
    ) -> "_FuncAsTransformer":
        if schema is None:
            schema = parse_output_schema_from_comment(func)
        if isinstance(schema, Schema):  # to be less strict on determinism
            schema = str(schema)
        validation_rules.update(parse_validation_rules_from_comment(func))
        assert_arg_not_none(schema, "schema")
        tr = _FuncAsTransformer()
        tr._wrapper = FunctionWrapper(func, "^[lsp]x*z?$", "^[lspq]$")  # type: ignore
        tr._output_schema_arg = schema  # type: ignore
        tr._validation_rules = validation_rules  # type: ignore
        return tr


class _FuncAsOutputTransformer(_FuncAsTransformer):
    def get_output_schema(self, df: DataFrame) -> Any:
        return OUTPUT_TRANSFORMER_DUMMY_SCHEMA

    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        self._wrapper.run(  # type: ignore
            [df], self.params, ignore_unknown=False, output=False
        )
        return ArrayDataFrame([], OUTPUT_TRANSFORMER_DUMMY_SCHEMA)

    @staticmethod
    def from_func(
        func: Callable, schema: Any, validation_rules: Dict[str, Any]
    ) -> "_FuncAsOutputTransformer":
        assert_or_throw(schema is None, "schema must be None for output transformers")
        validation_rules.update(parse_validation_rules_from_comment(func))
        tr = _FuncAsOutputTransformer()
        tr._wrapper = FunctionWrapper(func, "^[lsp]x*z?$", "^[lspnq]$")  # type: ignore
        tr._output_schema_arg = None  # type: ignore
        tr._validation_rules = validation_rules  # type: ignore
        return tr


class _FuncAsCoTransformer(CoTransformer):
    def get_output_schema(self, dfs: DataFrames) -> Any:
        return self._parse_schema(self._output_schema_arg, dfs)  # type: ignore

    @property
    def validation_rules(self) -> ParamDict:
        return self._validation_rules  # type: ignore

    @no_type_check
    def transform(self, dfs: DataFrames) -> LocalDataFrame:
        if self._dfs_input:  # function has DataFrames input
            return self._wrapper.run(  # type: ignore
                [dfs],
                self.params,
                ignore_unknown=False,
                output_schema=self.output_schema,
            )
        if not dfs.has_key:  # input does not have key
            return self._wrapper.run(  # type: ignore
                list(dfs.values()),
                self.params,
                ignore_unknown=False,
                output_schema=self.output_schema,
            )
        else:  # input DataFrames has key
            p = dict(dfs)
            p.update(self.params)
            return self._wrapper.run(  # type: ignore
                [], p, ignore_unknown=False, output_schema=self.output_schema
            )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrapper(*args, **kwargs)  # type: ignore

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(
            self._wrapper.__uuid__(), self._output_schema_arg, self._dfs_input
        )

    def _parse_schema(self, obj: Any, dfs: DataFrames) -> Schema:
        if callable(obj):
            return obj(dfs, **self.params)
        if isinstance(obj, str):
            return Schema(obj)
        if isinstance(obj, List):
            s = Schema()
            for x in obj:
                s += self._parse_schema(x, dfs)
            return s
        return Schema(obj)

    @staticmethod
    def from_func(
        func: Callable, schema: Any, validation_rules: Dict[str, Any]
    ) -> "_FuncAsCoTransformer":
        assert_or_throw(
            len(validation_rules) == 0,
            NotImplementedError("CoTransformer does not support validation rules"),
        )

        if schema is None:
            schema = parse_output_schema_from_comment(func)
        if isinstance(schema, Schema):  # to be less strict on determinism
            schema = str(schema)
        if isinstance(schema, str):
            assert_or_throw(
                "*" not in schema,
                FugueInterfacelessError(
                    "* can't be used on cotransformer output schema"
                ),
            )
        assert_arg_not_none(schema, "schema")
        tr = _FuncAsCoTransformer()
        tr._wrapper = FunctionWrapper(  # type: ignore
            func, "^(c|[lsp]+)x*z?$", "^[lspq]$"
        )
        tr._dfs_input = tr._wrapper.input_code[0] == "c"  # type: ignore
        tr._output_schema_arg = schema  # type: ignore
        tr._validation_rules = {}  # type: ignore
        return tr


class _FuncAsOutputCoTransformer(_FuncAsCoTransformer):
    def get_output_schema(self, dfs: DataFrames) -> Any:
        return OUTPUT_TRANSFORMER_DUMMY_SCHEMA

    @no_type_check
    def transform(self, dfs: DataFrames) -> LocalDataFrame:
        if self._dfs_input:  # function has DataFrames input
            self._wrapper.run(  # type: ignore
                [dfs],
                self.params,
                ignore_unknown=False,
                output=False,
            )
        elif not dfs.has_key:  # input does not have key
            self._wrapper.run(  # type: ignore
                list(dfs.values()),
                self.params,
                ignore_unknown=False,
                output=False,
            )
        else:  # input DataFrames has key
            p = dict(dfs)
            p.update(self.params)
            self._wrapper.run([], p, ignore_unknown=False, output=False)  # type: ignore
        return ArrayDataFrame([], OUTPUT_TRANSFORMER_DUMMY_SCHEMA)

    @staticmethod
    def from_func(
        func: Callable, schema: Any, validation_rules: Dict[str, Any]
    ) -> "_FuncAsOutputCoTransformer":
        assert_or_throw(schema is None, "schema must be None for output cotransformers")
        assert_or_throw(
            len(validation_rules) == 0,
            NotImplementedError("CoTransformer does not support validation rules"),
        )

        tr = _FuncAsOutputCoTransformer()
        tr._wrapper = FunctionWrapper(  # type: ignore
            func, "^(c|[lsp]+)x*z?$", "^[lspnq]$"
        )
        tr._dfs_input = tr._wrapper.input_code[0] == "c"  # type: ignore
        tr._output_schema_arg = None  # type: ignore
        tr._validation_rules = {}  # type: ignore
        return tr


def _to_transformer(  # noqa: C901
    obj: Any,
    schema: Any = None,
    global_vars: Optional[Dict[str, Any]] = None,
    local_vars: Optional[Dict[str, Any]] = None,
    validation_rules: Optional[Dict[str, Any]] = None,
    func_transformer_type: Type = _FuncAsTransformer,
    func_cotransformer_type: Type = _FuncAsCoTransformer,
) -> Union[Transformer, CoTransformer]:
    global_vars, local_vars = get_caller_global_local_vars(global_vars, local_vars)
    exp: Optional[Exception] = None
    if validation_rules is None:
        validation_rules = {}
    try:
        return copy.copy(
            to_instance(
                obj, Transformer, global_vars=global_vars, local_vars=local_vars
            )
        )
    except Exception as e:
        exp = e
    try:
        return copy.copy(
            to_instance(
                obj, CoTransformer, global_vars=global_vars, local_vars=local_vars
            )
        )
    except Exception as e:
        exp = e
    try:
        f = to_function(obj, global_vars=global_vars, local_vars=local_vars)
        # this is for string expression of function with decorator
        if isinstance(f, Transformer):
            return copy.copy(f)
        # this is for functions without decorator
        return func_transformer_type.from_func(
            f, schema, validation_rules=validation_rules
        )
    except Exception as e:
        exp = e
    try:
        f = to_function(obj, global_vars=global_vars, local_vars=local_vars)
        # this is for string expression of function with decorator
        if isinstance(f, CoTransformer):
            return copy.copy(f)
        # this is for functions without decorator
        return func_cotransformer_type.from_func(
            f, schema, validation_rules=validation_rules
        )
    except Exception as e:
        exp = e
    raise FugueInterfacelessError(f"{obj} is not a valid transformer", exp)


def _to_output_transformer(
    obj: Any,
    global_vars: Optional[Dict[str, Any]] = None,
    local_vars: Optional[Dict[str, Any]] = None,
    validation_rules: Optional[Dict[str, Any]] = None,
) -> Union[Transformer, CoTransformer]:
    global_vars, local_vars = get_caller_global_local_vars(global_vars, local_vars)
    return _to_transformer(
        obj=obj,
        schema=None,
        global_vars=global_vars,
        local_vars=local_vars,
        validation_rules=validation_rules,
        func_transformer_type=_FuncAsOutputTransformer,
        func_cotransformer_type=_FuncAsOutputCoTransformer,
    )
