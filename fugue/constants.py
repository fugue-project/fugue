from typing import Dict, Any

KEYWORD_ROWCOUNT = "ROWCOUNT"
KEYWORD_CORECOUNT = "CORECOUNT"

FUGUE_CONF_WORKFLOW_CONCURRENCY = "fugue.workflow.concurrency"
FUGUE_CONF_WORKFLOW_CHECKPOINT_PATH = "fugue.workflow.checkpoint.path"
FUGUE_CONF_WORKFLOW_AUTO_PERSIST = "fugue.workflow.auto_persist"
FUGUE_CONF_WORKFLOW_AUTO_PERSIST_VALUE = "fugue.workflow.auto_persist_value"
FUGUE_CONF_CACHE_PATH = "fugue.workflow.cache.path"

FUGUE_DEFAULT_CONF: Dict[str, Any] = {
    FUGUE_CONF_WORKFLOW_CONCURRENCY: 1,
    FUGUE_CONF_WORKFLOW_AUTO_PERSIST: False,
}
