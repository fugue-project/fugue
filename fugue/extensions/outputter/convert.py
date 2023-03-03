import copy
from typing import Any, Callable, Dict, List, Optional, no_type_check

from triad import ParamDict, to_uuid
from triad.utils.convert import get_caller_global_local_vars, to_function, to_instance

from fugue._utils.registry import fugue_plugin
from fugue.dataframe import DataFrames
from fugue.dataframe.function_wrapper import DataFrameFunctionWrapper
from fugue.exceptions import FugueInterfacelessError
from fugue.extensions._utils import (
    load_namespace_extensions,
    parse_validation_rules_from_comment,
    to_validation_rules,
)
from fugue.extensions.outputter.outputter import Outputter

_OUTPUTTER_REGISTRY = ParamDict()


@fugue_plugin
def parse_outputter(obj: Any) -> Any:
    """Parse an object to another object that can be converted to a Fugue
    :class:`~fugue.extensions.outputter.outputter.Outputter`.

    .. admonition:: Examples

        .. code-block:: python

            from fugue import Outputter, parse_outputter, FugueWorkflow
            from triad import to_uuid

            class My(Outputter):
                def __init__(self, x):
                    self.x = x

                def process(self, dfs):
                    raise NotImplementedError

                def __uuid__(self) -> str:
                    return to_uuid(super().__uuid__(), self.x)

            @parse_outputter.candidate(
                lambda x: isinstance(x, str) and x.startswith("-*"))
            def _parse(obj):
                return My(obj)

            dag = FugueWorkflow()
            dag.df([[0]], "a:int").output("-*abc")
            # ==  dag.df([[0]], "a:int").output(My("-*abc"))

            dag.run()
    """
    if isinstance(obj, str) and obj in _OUTPUTTER_REGISTRY:
        return _OUTPUTTER_REGISTRY[obj]
    return obj


def register_outputter(alias: str, obj: Any, on_dup: int = ParamDict.OVERWRITE) -> None:
    """Register outputter with an alias.

    :param alias: alias of the processor
    :param obj: the object that can be converted to
        :class:`~fugue.extensions.outputter.outputter.Outputter`
    :param on_dup: see :meth:`triad.collections.dict.ParamDict.update`
        , defaults to ``ParamDict.OVERWRITE``

    .. tip::

        Registering an extension with an alias is particularly useful for projects
        such as libraries. This is because by using alias, users don't have to
        import the specific extension, or provide the full path of the extension.
        It can make user's code less dependent and easy to understand.

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. seealso::

        Please read
        :doc:`Outputter Tutorial <tutorial:tutorials/extensions/outputter>`

    .. admonition:: Examples

        Here is an example how you setup your project so your users can
        benefit from this feature. Assume your project name is ``pn``

        The processor implementation in file ``pn/pn/outputters.py``

        .. code-block:: python

            from fugue import DataFrame

            def my_outputter(df:DataFrame) -> None:
                print(df)

        Then in ``pn/pn/__init__.py``

        .. code-block:: python

            from .outputters import my_outputter
            from fugue import register_outputter

            def register_extensions():
                register_outputter("mo", my_outputter)
                # ... register more extensions

            register_extensions()

        In users code:

        .. code-block:: python

            import pn  # register_extensions will be called
            from fugue import FugueWorkflow

            dag = FugueWorkflow()
            # use my_outputter by alias
            dag.df([[0]],"a:int").output("mo")
            dag.run()
    """
    _OUTPUTTER_REGISTRY.update({alias: obj}, on_dup=on_dup)


def outputter(**validation_rules: Any) -> Callable[[Any], "_FuncAsOutputter"]:
    """Decorator for outputters

    Please read
    :doc:`Outputter Tutorial <tutorial:tutorials/extensions/outputter>`
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
    load_namespace_extensions(obj)
    obj = parse_outputter(obj)
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
        tr._wrapper = DataFrameFunctionWrapper(  # type: ignore
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
