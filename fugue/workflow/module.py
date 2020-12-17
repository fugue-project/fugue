import copy
import inspect
from typing import Any, Callable, Dict, Iterable, Optional

from fugue._utils.interfaceless import FunctionWrapper, _FuncParam
from fugue.exceptions import FugueInterfacelessError
from fugue.workflow.workflow import FugueWorkflow, WorkflowDataFrame, WorkflowDataFrames
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import get_caller_global_local_vars, to_function


def module() -> Callable[[Any], "_ModuleFunctionWrapper"]:
    """Decorator for module

    Please read :ref:`Module Tutorial <tutorial:/tutorials/module.ipynb>`
    """

    def deco(func: Callable) -> "_ModuleFunctionWrapper":
        return _ModuleFunctionWrapper(func)

    return deco


def _to_module(
    obj: Any,
    global_vars: Optional[Dict[str, Any]] = None,
    local_vars: Optional[Dict[str, Any]] = None,
) -> "_ModuleFunctionWrapper":
    if isinstance(obj, _ModuleFunctionWrapper):
        return obj
    global_vars, local_vars = get_caller_global_local_vars(global_vars, local_vars)
    try:
        f = to_function(obj, global_vars=global_vars, local_vars=local_vars)
        # this is for string expression of function with decorator
        if isinstance(f, _ModuleFunctionWrapper):
            return copy.copy(f)
        # this is for functions without decorator
        return _ModuleFunctionWrapper(f)
    except Exception as e:
        exp = e
    raise FugueInterfacelessError(f"{obj} is not a valid module", exp)


class _FugueWorkflowParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "FugueWorkflow", "w")


class _WorkflowDataFrameParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "WorkflowDataFrame", "v")


class _WorkflowDataFramesParam(_FuncParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, "WorkflowDataFrame", "u")


class _ModuleFunctionWrapper(FunctionWrapper):
    def __init__(
        self,
        func: Callable,
        params_re: str = "^(w?(u|v+)|w(u?|v*))x*z?$",
        return_re: str = "^[uvn]$",
    ):
        super().__init__(func, params_re, return_re)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        if self._need_add_workflow(*args, **kwargs):
            wf = self._infer_workflow(*args, **kwargs)
            assert_or_throw(wf is not None, ValueError("can't infer workflow"))
            return super().__call__(wf, *args, **kwargs)
        return super().__call__(*args, **kwargs)

    @property
    def has_input(self) -> bool:
        return any(
            isinstance(x, (_WorkflowDataFrameParam, _WorkflowDataFramesParam))
            for x in self._params.values()
        )

    @property
    def has_dfs_input(self) -> bool:
        return any(
            isinstance(x, _WorkflowDataFramesParam) for x in self._params.values()
        )

    @property
    def has_no_output(self) -> bool:
        return not isinstance(
            self._rt, (_WorkflowDataFrameParam, _WorkflowDataFramesParam)
        )

    @property
    def has_single_output(self) -> bool:
        return isinstance(self._rt, _WorkflowDataFrameParam)

    @property
    def has_multiple_output(self) -> bool:
        return isinstance(self._rt, _WorkflowDataFramesParam)

    @property
    def _first_annotation_is_workflow(self) -> bool:
        return isinstance(self._params.get_value_by_index(0), _FugueWorkflowParam)

    def _need_add_workflow(self, *args: Any, **kwargs: Any):
        if not self._first_annotation_is_workflow:
            return False
        if self._params.get_key_by_index(0) in kwargs:
            return False
        if len(args) > 0 and isinstance(args[0], FugueWorkflow):
            return False
        return True

    def _infer_workflow(self, *args: Any, **kwargs: Any) -> Optional[FugueWorkflow]:
        def select_args() -> Iterable[Any]:
            for a in args:
                if isinstance(a, (WorkflowDataFrames, WorkflowDataFrame)):
                    yield a
            for _, v in kwargs.items():
                if isinstance(v, (WorkflowDataFrames, WorkflowDataFrame)):
                    yield v

        wf: Optional[FugueWorkflow] = None
        for a in select_args():
            if isinstance(a, WorkflowDataFrame):
                assert_or_throw(
                    wf is None or a.workflow is wf,
                    ValueError("different parenet workflows found on input dataframes"),
                )
                wf = a.workflow
            elif isinstance(a, WorkflowDataFrames):
                for k, v in a.items():
                    assert_or_throw(
                        isinstance(v, WorkflowDataFrame),
                        ValueError(f"{k}:{v} is not a WorkflowDataFrame"),
                    )
                    assert_or_throw(
                        wf is None or v.workflow is wf,
                        ValueError(
                            "different parenet workflows found on input dataframes"
                        ),
                    )
                    wf = v.workflow
        return wf

    def _parse_param(
        self,
        annotation: Any,
        param: Optional[inspect.Parameter],
        none_as_other: bool = True,
    ) -> _FuncParam:
        if issubclass(annotation, FugueWorkflow):
            # to prevent cyclic import
            return _FugueWorkflowParam(param)
        elif annotation is WorkflowDataFrame:
            # to prevent cyclic import
            return _WorkflowDataFrameParam(param)
        elif annotation is WorkflowDataFrames:
            # to prevent cyclic import
            return _WorkflowDataFramesParam(param)
        else:
            return super()._parse_param(annotation, param, none_as_other)
