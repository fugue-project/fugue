from typing import Dict, Any

FUGUE_CONTRIB: Dict[str, Any] = {
    "viz": {"module": "fugue_contrib.viz"},
    "sns": {"module": "fugue_contrib.seaborn"},
    "why": {"module": "whylogs.api.fugue.registry"},
    "vizzu": {"module": "ipyvizzu.integrations.fugue"},
}
