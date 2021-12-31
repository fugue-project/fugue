from typing import Any, Dict

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

FUGUE_DEFAULT_CONF: Dict[str, Any] = {
    FUGUE_CONF_WORKFLOW_CONCURRENCY: 1,
    FUGUE_CONF_WORKFLOW_AUTO_PERSIST: False,
    FUGUE_CONF_WORKFLOW_EXCEPTION_HIDE: "fugue.,six,adagio.,fugue_dask.,dask.,"
    "fugue_spark.,pyspark.,antlr4,_qpd_antlr,qpd,triad,ipython.,jupyter.,"
    "ipykernel,_pytest,pytest",
    FUGUE_CONF_WORKFLOW_EXCEPTION_INJECT: 3,
    FUGUE_CONF_WORKFLOW_EXCEPTION_OPTIMIZE: True,
    FUGUE_CONF_SQL_IGNORE_CASE: False,
}

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
