import copy
from typing import Any, Callable, Dict, List, Optional, no_type_check

from fugue._utils.interfaceless import FunctionWrapper, parse_output_schema_from_comment
from fugue.dataframe import DataFrame, DataFrames
from fugue.exceptions import FugueInterfacelessError
from fugue.extensions._utils import (
    parse_validation_rules_from_comment,
    to_validation_rules,
)
from fugue.extensions.processor.processor import Processor
from triad.collections import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import get_caller_global_local_vars, to_function, to_instance
from triad.utils.hash import to_uuid


def processor(
    schema: Any = None, **validation_rules: Any
) -> Callable[[Any], "_FuncAsProcessor"]:
    """Decorator for processors

    Please read :ref:`Processor Tutorial <tutorial:/tutorials/processor.ipynb>`
    """
    # TODO: validation of schema if without * should be done at compile time
    def deco(func: Callable) -> "_FuncAsProcessor":
        return _FuncAsProcessor.from_func(
            func, schema, validation_rules=to_validation_rules(validation_rules)
        )

    return deco


def _to_processor(
    obj: Any,
    schema: Any = None,
    global_vars: Optional[Dict[str, Any]] = None,
    local_vars: Optional[Dict[str, Any]] = None,
    validation_rules: Optional[Dict[str, Any]] = None,
) -> Processor:
    global_vars, local_vars = get_caller_global_local_vars(global_vars, local_vars)
    exp: Optional[Exception] = None
    if validation_rules is None:
        validation_rules = {}
    try:
        return copy.copy(
            to_instance(obj, Processor, global_vars=global_vars, local_vars=local_vars)
        )
    except Exception as e:
        exp = e
    try:
        f = to_function(obj, global_vars=global_vars, local_vars=local_vars)
        # this is for string expression of function with decorator
        if isinstance(f, Processor):
            return copy.copy(f)
        # this is for functions without decorator
        return _FuncAsProcessor.from_func(f, schema, validation_rules=validation_rules)
    except Exception as e:
        exp = e
    raise FugueInterfacelessError(f"{obj} is not a valid processor", exp)


class _FuncAsProcessor(Processor):
    @property
    def validation_rules(self) -> Dict[str, Any]:
        return self._validation_rules  # type: ignore

    @no_type_check
    def process(self, dfs: DataFrames) -> DataFrame:
        args: List[Any] = []
        kwargs: Dict[str, Any] = {}
        if self._need_engine:
            args.append(self.execution_engine)
        if self._use_dfs:
            args.append(dfs)
        else:
            if not dfs.has_key:
                args += dfs.values()
            else:
                kwargs.update(dfs)
        kwargs.update(self.params)
        return self._wrapper.run(
            args=args,
            kwargs=kwargs,
            output_schema=self.output_schema if self._need_output_schema else None,
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrapper(*args, **kwargs)  # type: ignore

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(
            self._wrapper,
            self._need_engine,
            self._use_dfs,
            self._need_output_schema,
            str(self._output_schema),
        )

    @no_type_check
    @staticmethod
    def from_func(
        func: Callable, schema: Any, validation_rules: Dict[str, Any]
    ) -> "_FuncAsProcessor":
        if schema is None:
            schema = parse_output_schema_from_comment(func)
        validation_rules.update(parse_validation_rules_from_comment(func))
        tr = _FuncAsProcessor()
        tr._wrapper = FunctionWrapper(
            func, "^e?(c|[dlspq]+)x*z?$", "^[dlspq]$"
        )  # type: ignore
        tr._need_engine = tr._wrapper.input_code.startswith("e")
        tr._use_dfs = "c" in tr._wrapper.input_code
        tr._need_output_schema = "s" == tr._wrapper.output_code
        tr._validation_rules = validation_rules
        tr._output_schema = Schema(schema)
        if len(tr._output_schema) == 0:
            assert_or_throw(
                not tr._need_output_schema,
                FugueInterfacelessError(
                    f"schema must be provided for return type {tr._wrapper._rt}"
                ),
            )
        else:
            assert_or_throw(
                tr._need_output_schema,
                FugueInterfacelessError(
                    f"schema must not be provided for return type {tr._wrapper._rt}"
                ),
            )
        return tr
