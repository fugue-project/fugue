import copy
from typing import Any, Callable, Dict, List, Optional, no_type_check

from fugue.extensions.creator.creator import Creator
from fugue.dataframe import DataFrame
from fugue.exceptions import FugueInterfacelessError
from fugue._utils.interfaceless import FunctionWrapper, parse_output_schema_from_comment
from triad.collections import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import to_function, to_instance, get_caller_global_local_vars
from triad.utils.hash import to_uuid


def creator(schema: Any = None) -> Callable[[Any], "_FuncAsCreator"]:
    """Decorator for creators

    Please read :ref:`Creator Tutorial <tutorial:/tutorials/creator.ipynb>`
    """

    def deco(func: Callable) -> "_FuncAsCreator":
        return _FuncAsCreator.from_func(func, schema)

    return deco


def _to_creator(
    obj: Any,
    schema: Any = None,
    global_vars: Optional[Dict[str, Any]] = None,
    local_vars: Optional[Dict[str, Any]] = None,
) -> Creator:
    global_vars, local_vars = get_caller_global_local_vars(global_vars, local_vars)
    exp: Optional[Exception] = None
    try:
        return copy.copy(
            to_instance(obj, Creator, global_vars=global_vars, local_vars=local_vars)
        )
    except Exception as e:
        exp = e
    try:
        f = to_function(obj, global_vars=global_vars, local_vars=local_vars)
        # this is for string expression of function with decorator
        if isinstance(f, Creator):
            return copy.copy(f)
        # this is for functions without decorator
        return _FuncAsCreator.from_func(f, schema)
    except Exception as e:
        exp = e
    raise FugueInterfacelessError(f"{obj} is not a valid creator", exp)


class _FuncAsCreator(Creator):
    @no_type_check
    def create(self) -> DataFrame:
        args: List[Any] = []
        kwargs: Dict[str, Any] = {}
        if self._need_engine:  # type: ignore
            args.append(self.execution_engine)
        kwargs.update(self.params)
        return self._wrapper.run(  # type: ignore
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
            self._need_output_schema,
            str(self._output_schema),
        )

    @no_type_check
    @staticmethod
    def from_func(func: Callable, schema: Any) -> "_FuncAsCreator":
        # pylint: disable=W0201
        if schema is None:
            schema = parse_output_schema_from_comment(func)
        tr = _FuncAsCreator()
        tr._wrapper = FunctionWrapper(func, "^e?x*z?$", "^[dlspq]$")  # type: ignore
        tr._need_engine = tr._wrapper.input_code.startswith("e")
        tr._need_output_schema = "s" == tr._wrapper.output_code
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
