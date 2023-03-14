import copy
from typing import Any, Callable, Dict, List, Optional, no_type_check

from triad import ParamDict
from triad.collections import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import get_caller_global_local_vars, to_function, to_instance
from triad.utils.hash import to_uuid

from fugue._utils.interfaceless import parse_output_schema_from_comment
from fugue._utils.registry import fugue_plugin
from fugue.dataframe import DataFrame
from fugue.dataframe.function_wrapper import DataFrameFunctionWrapper
from fugue.exceptions import FugueInterfacelessError
from fugue.extensions.creator.creator import Creator

from .._utils import load_namespace_extensions

_CREATOR_REGISTRY = ParamDict()


@fugue_plugin
def parse_creator(obj: Any) -> Any:
    """Parse an object to another object that can be converted to a Fugue
    :class:`~fugue.extensions.creator.creator.Creator`.

    .. admonition:: Examples

        .. code-block:: python

            from fugue import Creator, FugueWorkflow
            from fugue.plugins import parse_creator
            from triad import to_uuid

            class My(Creator):
                def __init__(self, x):
                    self.x = x

                def create(self) :
                    raise NotImplementedError

                def __uuid__(self) -> str:
                    return to_uuid(super().__uuid__(), self.x)

            @parse_creator.candidate(
                lambda x: isinstance(x, str) and x.startswith("-*"))
            def _parse(obj):
                return My(obj)

            dag = FugueWorkflow()
            dag.create("-*abc").show()
            # == dag.create(My("-*abc")).show()

            dag.run()
    """
    if isinstance(obj, str) and obj in _CREATOR_REGISTRY:
        return _CREATOR_REGISTRY[obj]
    return obj


def register_creator(alias: str, obj: Any, on_dup: int = ParamDict.OVERWRITE) -> None:
    """Register creator with an alias. This is a simplified version of
    :func:`~.parse_creator`

    :param alias: alias of the creator
    :param obj: the object that can be converted to
        :class:`~fugue.extensions.creator.creator.Creator`
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
        :doc:`Creator Tutorial <tutorial:tutorials/extensions/creator>`

    .. admonition:: Examples

        Here is an example how you setup your project so your users can
        benefit from this feature. Assume your project name is ``pn``

        The creator implementation in file ``pn/pn/creators.py``

        .. code-block:: python

            import pandas import pd

            def my_creator() -> pd.DataFrame:
                return pd.DataFrame()

        Then in ``pn/pn/__init__.py``

        .. code-block:: python

            from .creators import my_creator
            from fugue import register_creator

            def register_extensions():
                register_creator("mc", my_creator)
                # ... register more extensions

            register_extensions()

        In users code:

        .. code-block:: python

            import pn  # register_extensions will be called
            from fugue import FugueWorkflow

            dag = FugueWorkflow()
            dag.create("mc").show()  # use my_creator by alias
            dag.run()
    """
    _CREATOR_REGISTRY.update({alias: obj}, on_dup=on_dup)


def creator(schema: Any = None) -> Callable[[Any], "_FuncAsCreator"]:
    """Decorator for creators

    Please read
    :doc:`Creator Tutorial <tutorial:tutorials/extensions/creator>`
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
    load_namespace_extensions(obj)
    obj = parse_creator(obj)
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
        if self._engine_param is not None:
            args.append(self._engine_param.to_input(self.execution_engine))
        kwargs.update(self.params)
        return self._wrapper.run(  # type: ignore
            args=args,
            kwargs=kwargs,
            output_schema=self.output_schema if self._need_output_schema else None,
            ctx=self.execution_engine,
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._wrapper(*args, **kwargs)  # type: ignore

    @no_type_check
    def __uuid__(self) -> str:
        return to_uuid(
            self._wrapper,
            self._engine_param,
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
        tr._wrapper = DataFrameFunctionWrapper(  # type: ignore
            func, "^e?x*z?$", "^[dlspq]$"
        )
        tr._engine_param = (
            tr._wrapper._params.get_value_by_index(0)
            if tr._wrapper.input_code.startswith("e")
            else None
        )
        tr._need_output_schema = tr._wrapper.need_output_schema
        tr._output_schema = Schema(schema)
        if len(tr._output_schema) == 0:
            assert_or_throw(
                tr._need_output_schema is None or not tr._need_output_schema,
                FugueInterfacelessError(
                    f"schema must be provided for return type {tr._wrapper._rt}"
                ),
            )
        else:
            assert_or_throw(
                tr._need_output_schema is None or tr._need_output_schema,
                FugueInterfacelessError(
                    f"schema must not be provided for return type {tr._wrapper._rt}"
                ),
            )
        return tr
