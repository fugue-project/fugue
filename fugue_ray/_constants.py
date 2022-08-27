from typing import Dict, Any

FUGUE_RAY_CONF_SHUFFLE_PARTITIONS = "fugue.ray.shuffle.partitions"

FUGUE_RAY_DEFAULT_CONF: Dict[str, Any] = {FUGUE_RAY_CONF_SHUFFLE_PARTITIONS: -1}
