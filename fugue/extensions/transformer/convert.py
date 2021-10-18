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
    ExtensionRegistry,
    parse_validation_rules_from_comment,
    to_validation_rules,
)
from fugue.extensions.transformer.constants import OUTPUT_TRANSFORMER_DUMMY_SCHEMA
from fugue.extensions.transformer.transformer import CoTransformer, Transformer
from triad import ParamDict, Schema
from triad.utils.assertion import assert_arg_not_none, assert_or_throw
from triad.utils.convert import get_caller_global_local_vars, to_function, to_instance
from triad.utils.hash import to_uuid

_TRANSFORMER_REGISTRY = ExtensionRegistry()
_OUT_TRANSFORMER_REGISTRY = ExtensionRegistry()


def register_transformer(alias: str, obj: Any, on_dup: str = "overwrite") -> None:
    """Register transformer with an alias.

    :param alias: alias of the transformer
    :param obj: the object that can be converted to
        :class:`~fugue.extensions.transformer.transformer.Transformer` or
        :class:`~fugue.extensions.transformer.transformer.CoTransformer`
    :param on_dup: action on duplicated ``alias``. It can be "overwrite", "ignore"
        (not overwriting) or "throw" (throw exception), defaults to "overwrite".

    .. tip::

        Registering an extension with an alias is particularly useful for projects
        such as libraries. This is because by using alias, users don't have to
        import the specific extension, or provide the full path of the extension.
        It can make user's code less dependent and easy to understand.

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. seealso::

        Please read |TransformerTutorial|

    .. admonition:: Examples

        Here is an example how you setup your project so your users can
        benefit from this feature. Assume your project name is ``pn``

        The transformer implementation in file ``pn/pn/transformers.py``

        .. code-block:: python

            import pandas as pd

            # schema: *
            def my_transformer(df:pd.DataFrame) -> pd.DataFrame:
                return df

        Then in ``pn/pn/__init__.py``

        .. code-block:: python

            from .transformers import my_transformer
            from fugue import register_transformer

            def register_extensions():
                register_transformer("mt", my_transformer)
                # ... register more extensions

            register_extensions()

        In users code:

        .. code-block:: python

            import pn  # register_extensions will be called
            from fugue import FugueWorkflow

            with FugueWorkflow() as dag:
                # use my_transformer by alias
                dag.df([[0]],"a:int").transform("mt").show()
    """
    _TRANSFORMER_REGISTRY.register(alias, obj, on_dup=on_dup)


def register_output_transformer(
    alias: str, obj: Any, on_dup: str = "overwrite"
) -> None:
    """Register output transformer with an alias.

    :param alias: alias of the transformer
    :param obj: the object that can be converted to
        :class:`~fugue.extensions.transformer.transformer.OutputTransformer` or
        :class:`~fugue.extensions.transformer.transformer.OutputCoTransformer`
    :param on_dup: action on duplicated ``alias``. It can be "overwrite", "ignore"
        (not overwriting) or "throw" (throw exception), defaults to "overwrite".

    .. tip::

        Registering an extension with an alias is particularly useful for projects
        such as libraries. This is because by using alias, users don't have to
        import the specific extension, or provide the full path of the extension.
        It can make user's code less dependent and easy to understand.

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. seealso::

        Please read |TransformerTutorial|

    .. admonition:: Examples

        Here is an example how you setup your project so your users can
        benefit from this feature. Assume your project name is ``pn``

        The transformer implementation in file ``pn/pn/transformers.py``

        .. code-block:: python

            import pandas as pd

            def my_transformer(df:pd.DataFrame) -> None:
                df.to_parquet("<unique_path>")

        Then in ``pn/pn/__init__.py``

        .. code-block:: python

            from .transformers import my_transformer
            from fugue import register_transformer

            def register_extensions():
                register_transformer("mt", my_transformer)
                # ... register more extensions

            register_extensions()

        In users code:

        .. code-block:: python

            import pn  # register_extensions will be called
            from fugue import FugueWorkflow

            with FugueWorkflow() as dag:
                # use my_transformer by alias
                dag.df([[0]],"a:int").out_transform("mt")
    """
    _OUT_TRANSFORMER_REGISTRY.register(alias, obj, on_dup=on_dup)


def transformer(
    schema: Any, **validation_rules: Any
) -> Callable[[Any], "_FuncAsTransformer"]:
    """Decorator for transformers

    Please read |TransformerTutorial|
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

    Please read |TransformerTutorial|
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

    Please read |CoTransformerTutorial|
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

    Please read |CoTransformerTutorial|
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
    def validate_on_compile(self) -> None:
        super().validate_on_compile()
        _validate_callback(self)

    def get_output_schema(self, df: DataFrame) -> Any:
        return self._parse_schema(self._output_schema_arg, df)  # type: ignore

    @property
    def validation_rules(self) -> Dict[str, Any]:
        return self._validation_rules  # type: ignore

    @no_type_check
    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        args = [df] + _get_callback(self)
        return self._wrapper.run(
            args, self.params, ignore_unknown=False, output_schema=self.output_schema
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
        tr._wrapper = FunctionWrapper(  # type: ignore
            func, "^[lspq][fF]?x*z?$", "^[lspq]$"
        )
        tr._output_schema_arg = schema  # type: ignore
        tr._validation_rules = validation_rules  # type: ignore
        tr._uses_callback = "f" in tr._wrapper.input_code.lower()  # type: ignore
        tr._requires_callback = "F" in tr._wrapper.input_code  # type: ignore
        return tr


class _FuncAsOutputTransformer(_FuncAsTransformer):
    def validate_on_compile(self) -> None:
        super().validate_on_compile()
        _validate_callback(self)

    def get_output_schema(self, df: DataFrame) -> Any:
        return OUTPUT_TRANSFORMER_DUMMY_SCHEMA

    @no_type_check
    def transform(self, df: LocalDataFrame) -> LocalDataFrame:
        args = [df] + _get_callback(self)
        self._wrapper.run(args, self.params, ignore_unknown=False, output=False)
        return ArrayDataFrame([], OUTPUT_TRANSFORMER_DUMMY_SCHEMA)

    @staticmethod
    def from_func(
        func: Callable, schema: Any, validation_rules: Dict[str, Any]
    ) -> "_FuncAsOutputTransformer":
        assert_or_throw(schema is None, "schema must be None for output transformers")
        validation_rules.update(parse_validation_rules_from_comment(func))
        tr = _FuncAsOutputTransformer()
        tr._wrapper = FunctionWrapper(  # type: ignore
            func, "^[lspq][fF]?x*z?$", "^[lspnq]$"
        )
        tr._output_schema_arg = None  # type: ignore
        tr._validation_rules = validation_rules  # type: ignore
        tr._uses_callback = "f" in tr._wrapper.input_code.lower()  # type: ignore
        tr._requires_callback = "F" in tr._wrapper.input_code  # type: ignore
        return tr


class _FuncAsCoTransformer(CoTransformer):
    def validate_on_compile(self) -> None:
        super().validate_on_compile()
        _validate_callback(self)

    def get_output_schema(self, dfs: DataFrames) -> Any:
        return self._parse_schema(self._output_schema_arg, dfs)  # type: ignore

    @property
    def validation_rules(self) -> ParamDict:
        return self._validation_rules  # type: ignore

    @no_type_check
    def transform(self, dfs: DataFrames) -> LocalDataFrame:
        cb = _get_callback(self)
        if self._dfs_input:  # function has DataFrames input
            return self._wrapper.run(  # type: ignore
                [dfs] + cb,
                self.params,
                ignore_unknown=False,
                output_schema=self.output_schema,
            )
        if not dfs.has_key:  # input does not have key
            return self._wrapper.run(  # type: ignore
                list(dfs.values()) + cb,
                self.params,
                ignore_unknown=False,
                output_schema=self.output_schema,
            )
        else:  # input DataFrames has key
            p = dict(dfs)
            p.update(self.params)
            return self._wrapper.run(  # type: ignore
                [] + cb, p, ignore_unknown=False, output_schema=self.output_schema
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
            func, "^(c|[lspq]+)[fF]?x*z?$", "^[lspq]$"
        )
        tr._dfs_input = tr._wrapper.input_code[0] == "c"  # type: ignore
        tr._output_schema_arg = schema  # type: ignore
        tr._validation_rules = {}  # type: ignore
        tr._uses_callback = "f" in tr._wrapper.input_code.lower()  # type: ignore
        tr._requires_callback = "F" in tr._wrapper.input_code  # type: ignore
        return tr


class _FuncAsOutputCoTransformer(_FuncAsCoTransformer):
    def validate_on_compile(self) -> None:
        super().validate_on_compile()
        _validate_callback(self)

    def get_output_schema(self, dfs: DataFrames) -> Any:
        return OUTPUT_TRANSFORMER_DUMMY_SCHEMA

    @no_type_check
    def transform(self, dfs: DataFrames) -> LocalDataFrame:
        cb = _get_callback(self)
        if self._dfs_input:  # function has DataFrames input
            self._wrapper.run(  # type: ignore
                [dfs] + cb,
                self.params,
                ignore_unknown=False,
                output=False,
            )
        elif not dfs.has_key:  # input does not have key
            self._wrapper.run(  # type: ignore
                list(dfs.values()) + cb,
                self.params,
                ignore_unknown=False,
                output=False,
            )
        else:  # input DataFrames has key
            p = dict(dfs)
            p.update(self.params)
            self._wrapper.run(
                [] + cb, p, ignore_unknown=False, output=False  # type: ignore
            )
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
            func, "^(c|[lspq]+)[fF]?x*z?$", "^[lspnq]$"
        )
        tr._dfs_input = tr._wrapper.input_code[0] == "c"  # type: ignore
        tr._output_schema_arg = None  # type: ignore
        tr._validation_rules = {}  # type: ignore
        tr._uses_callback = "f" in tr._wrapper.input_code.lower()  # type: ignore
        tr._requires_callback = "F" in tr._wrapper.input_code  # type: ignore
        return tr


def _to_transformer(
    obj: Any,
    schema: Any = None,
    global_vars: Optional[Dict[str, Any]] = None,
    local_vars: Optional[Dict[str, Any]] = None,
    validation_rules: Optional[Dict[str, Any]] = None,
) -> Union[Transformer, CoTransformer]:
    global_vars, local_vars = get_caller_global_local_vars(global_vars, local_vars)
    return _to_general_transformer(
        obj=_TRANSFORMER_REGISTRY.get(obj),
        schema=schema,
        global_vars=global_vars,
        local_vars=local_vars,
        validation_rules=validation_rules,
        func_transformer_type=_FuncAsTransformer,
        func_cotransformer_type=_FuncAsCoTransformer,
    )


def _to_output_transformer(
    obj: Any,
    global_vars: Optional[Dict[str, Any]] = None,
    local_vars: Optional[Dict[str, Any]] = None,
    validation_rules: Optional[Dict[str, Any]] = None,
) -> Union[Transformer, CoTransformer]:
    global_vars, local_vars = get_caller_global_local_vars(global_vars, local_vars)
    return _to_general_transformer(
        obj=_OUT_TRANSFORMER_REGISTRY.get(obj),
        schema=None,
        global_vars=global_vars,
        local_vars=local_vars,
        validation_rules=validation_rules,
        func_transformer_type=_FuncAsOutputTransformer,
        func_cotransformer_type=_FuncAsOutputCoTransformer,
    )


def _to_general_transformer(  # noqa: C901
    obj: Any,
    schema: Any,
    global_vars: Optional[Dict[str, Any]],
    local_vars: Optional[Dict[str, Any]],
    validation_rules: Optional[Dict[str, Any]],
    func_transformer_type: Type,
    func_cotransformer_type: Type,
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


def _validate_callback(ctx: Any) -> None:
    if ctx._requires_callback:
        assert_or_throw(
            ctx.has_callback,
            FugueInterfacelessError(f"Callback is required but not provided: {ctx}"),
        )


def _get_callback(ctx: Any) -> List[Any]:
    uses_callback = ctx._uses_callback
    requires_callback = ctx._requires_callback
    if not uses_callback:
        return []
    if requires_callback:
        assert_or_throw(
            ctx.has_callback,
            FugueInterfacelessError(f"Callback is required but not provided: {ctx}"),
        )
        return [ctx.callback]
    return [ctx.callback if ctx.has_callback else None]
