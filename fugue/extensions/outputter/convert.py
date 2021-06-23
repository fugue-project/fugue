import copy
from typing import Any, Callable, Dict, List, Optional, no_type_check

from fugue._utils.interfaceless import FunctionWrapper
from fugue.dataframe import DataFrames
from fugue.exceptions import FugueInterfacelessError
from fugue.extensions._utils import (
    ExtensionRegistry,
    parse_validation_rules_from_comment,
    to_validation_rules,
)
from fugue.extensions.outputter.outputter import Outputter
from triad.utils.convert import get_caller_global_local_vars, to_function, to_instance
from triad.utils.hash import to_uuid

_OUTPUTTER_REGISTRY = ExtensionRegistry()


def register_outputter(alias: str, obj: Any, overwrite: bool = False):
    _OUTPUTTER_REGISTRY.register(alias, obj, overwrite=overwrite)


def outputter(**validation_rules: Any) -> Callable[[Any], "_FuncAsOutputter"]:
    """Decorator for outputters

    Please read :ref:`Outputter Tutorial <tutorial:/tutorials/outputter.ipynb>`
    """

    def deco(func: Callable) -> "_FuncAsOutputter":
        return _FuncAsOutputter.from_func(
            func, validation_rules=to_validation_rules(validation_rules)
        )

    return deco


def _to_outputter(
    obj: Any,
    global_vars: Optional[Dict[str, Any]] = None,
    local_vars: Optional[Dict[str, Any]] = None,
    validation_rules: Optional[Dict[str, Any]] = None,
) -> Outputter:
    global_vars, local_vars = get_caller_global_local_vars(global_vars, local_vars)
    obj = _OUTPUTTER_REGISTRY.get(obj)
    exp: Optional[Exception] = None
    if validation_rules is None:
        validation_rules = {}
    try:
        return copy.copy(
            to_instance(obj, Outputter, global_vars=global_vars, local_vars=local_vars)
        )
    except Exception as e:
        exp = e
    try:
        f = to_function(obj, global_vars=global_vars, local_vars=local_vars)
        # this is for string expression of function with decorator
        if isinstance(f, Outputter):
            return copy.copy(f)
        # this is for functions without decorator
        return _FuncAsOutputter.from_func(f, validation_rules=validation_rules)
    except Exception as e:
        exp = e
    raise FugueInterfacelessError(f"{obj} is not a valid outputter", exp)


class _FuncAsOutputter(Outputter):
    @property
    def validation_rules(self) -> Dict[str, Any]:
        return self._validation_rules  # type: ignore

    @no_type_check
    def process(self, dfs: DataFrames) -> None:
        args: List[Any] = []
        kwargs: Dict[str, Any] = {}
        if self._engine_param is not None:
            args.append(self._engine_param.to_input(self.execution_engine))
        if self._use_dfs:
            args.append(dfs)
        else:
            if not dfs.has_key:
                args += dfs.values()
            else:
                kwargs.update(dfs)
        kwargs.update(self.params)
        return self._wrapper.run(args=args, kwargs=kwargs, ctx=self.execution_engine)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrapper(*args, **kwargs)  # type: ignore

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(self._wrapper, self._engine_param, self._use_dfs)

    @no_type_check
    @staticmethod
    def from_func(
        func: Callable, validation_rules: Dict[str, Any]
    ) -> "_FuncAsOutputter":
        validation_rules.update(parse_validation_rules_from_comment(func))
        tr = _FuncAsOutputter()
        tr._wrapper = FunctionWrapper(  # type: ignore
            func, "^e?(c|[dlspq]+)x*z?$", "^n$"
        )
        tr._engine_param = (
            tr._wrapper._params.get_value_by_index(0)
            if tr._wrapper.input_code.startswith("e")
            else None
        )
        tr._use_dfs = "c" in tr._wrapper.input_code
        tr._validation_rules = validation_rules
        return tr
