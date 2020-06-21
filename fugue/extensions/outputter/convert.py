import copy
from typing import Any, Callable, List, Optional, Dict, no_type_check

from fugue.dataframe import DataFrames
from fugue.exceptions import FugueInterfacelessError
from triad.utils.convert import to_function, to_instance
from fugue.extensions.outputter.outputter import Outputter
from fugue._utils.interfaceless import FunctionWrapper
from triad.utils.hash import to_uuid


def outputter() -> Callable[[Any], "_FuncAsOutputter"]:
    """Decorator for outputters

    Please read :ref:`Outputter Tutorial <tutorial:/tutorials/outputter.ipynb>`
    """

    def deco(func: Callable) -> _FuncAsOutputter:
        return _FuncAsOutputter.from_func(func)

    return deco


def _to_outputter(obj: Any) -> Outputter:
    exp: Optional[Exception] = None
    try:
        return copy.copy(to_instance(obj, Outputter))
    except Exception as e:
        exp = e
    try:
        f = to_function(obj)
        # this is for string expression of function with decorator
        if isinstance(f, Outputter):
            return copy.copy(f)
        # this is for functions without decorator
        return _FuncAsOutputter.from_func(f)
    except Exception as e:
        exp = e
    raise FugueInterfacelessError(f"{obj} is not a valid outputter", exp)


class _FuncAsOutputter(Outputter):
    @no_type_check
    def process(self, dfs: DataFrames) -> None:
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
        return self._wrapper.run(args=args, kwargs=kwargs)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrapper(*args, **kwargs)  # type: ignore

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(self._wrapper, self._need_engine, self._use_dfs)

    @no_type_check
    @staticmethod
    def from_func(func: Callable) -> "_FuncAsOutputter":
        tr = _FuncAsOutputter()
        tr._wrapper = FunctionWrapper(func, "^e?(c|[dlsp]+)x*$", "^n$")  # type: ignore
        tr._need_engine = tr._wrapper.input_code.startswith("e")
        tr._use_dfs = "c" in tr._wrapper.input_code
        return tr
