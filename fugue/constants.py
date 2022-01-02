from typing import Any, Dict
from triad import ParamDict

KEYWORD_ROWCOUNT = "ROWCOUNT"
KEYWORD_CORECOUNT = "CORECOUNT"

FUGUE_CONF_WORKFLOW_CONCURRENCY = "fugue.workflow.concurrency"
FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH = "fugue.workflow.checkpoint.path"
FUGUE_CONF_WORKFLOW_AUTO_PERSIST = "fugue.workflow.auto_persist"
FUGUE_CONF_WORKFLOW_AUTO_PERSIST_VALUE = "fugue.workflow.auto_persist_value"
FUGUE_CONF_CACHE_PATH = "fugue.workflow.cache.path"
FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE = "fugue.workflow.exception.hide"
FUGUE_CONF_WORKFLOW_EXCEPTION_INJECT = "fugue.workflow.exception.inject"
FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE = "fugue.workflow.exception.optimize"
FUGUE_CONF_SQL_IGNORE_CASE = "fugue.sql.compile.ignore_case"


FUGUE_COMPILE_TIME_CONFIGS = set(
    [
        FUGUE_CONF_WORKFLOW_AUTO_PERSIST,
        FUGUE_CONF_WORKFLOW_AUTO_PERSIST_VALUE,
        FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE,
        FUGUE_CONF_WORKFLOW_EXCEPTION_INJECT,
        FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE,
        FUGUE_CONF_SQL_IGNORE_CASE,
    ]
)

_FUGUE_GLOBAL_CONF = ParamDict(
    {
        FUGUE_CONF_WORKFLOW_CONCURRENCY: 1,
        FUGUE_CONF_WORKFLOW_AUTO_PERSIST: False,
        FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE: "fugue.,six,adagio.,pandas,"
        "fugue_dask.,dask.,fugue_spark.,pyspark.,antlr4,_qpd_antlr,qpd,triad,"
        "fugue_notebook.,ipython.,jupyter.,ipykernel,_pytest,pytest,fugue_ibis.",
        FUGUE_CONF_WORKFLOW_EXCEPTION_INJECT: 3,
        FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE: True,
        FUGUE_CONF_SQL_IGNORE_CASE: False,
    }
)


def register_global_conf(
    conf: Dict[str, Any], on_dup: int = ParamDict.OVERWRITE
) -> None:
    """Register global Fugue configs that can be picked up by any
    Fugue execution engines as the base configs.

    :param conf: the config dictionary
    :param on_dup: see :meth:`triad.collections.dict.ParamDict.update`
      , defaults to ``ParamDict.OVERWRITE``

    .. note::

        When using ``ParamDict.THROW`` or ``on_dup``, it's transactional.
        If any key in ``conf`` is already in global config and the value
        is different from the new value, then ValueError will be thrown.

    .. admonition:: Examples

        .. code-block:: python

            from fugue import register_global_conf, NativeExecutionEngine

            register_global_conf({"my.value",1})

            engine = NativeExecutionEngine()
            assert 1 == engine.conf["my.value"]

            engine = NativeExecutionEngine({"my.value",2})
            assert 2 == engine.conf["my.value"]
    """
    if on_dup == ParamDict.THROW:
        # be transactional
        for k, v in conf.items():
            if k in _FUGUE_GLOBAL_CONF:
                vv = _FUGUE_GLOBAL_CONF[k]
                if vv != v:
                    raise ValueError(
                        f"for global config {k}, the existed "
                        f"value is {vv} and can't take new value {v}"
                    )
        on_dup = ParamDict.OVERWRITE
    _FUGUE_GLOBAL_CONF.update(conf, on_dup=on_dup, deep=True)
