from typing import Any, Dict

import ray

FUGUE_RAY_CONF_SHUFFLE_PARTITIONS = "fugue.ray.shuffle.partitions"
FUGUE_RAY_DEFAULT_PARTITIONS = "fugue.ray.default.partitions"
FUGUE_RAY_DEFAULT_BATCH_SIZE = "fugue.ray.default.batch_size"
FUGUE_RAY_ZERO_COPY = "fugue.ray.zero_copy"

FUGUE_RAY_DEFAULT_CONF: Dict[str, Any] = {
    FUGUE_RAY_CONF_SHUFFLE_PARTITIONS: -1,
    FUGUE_RAY_DEFAULT_PARTITIONS: 0,
    FUGUE_RAY_ZERO_COPY: True,
}

if ray.__version__ >= "2.3":
    _ZERO_COPY: Dict[str, Any] = {"zero_copy_batch": True}
else:  # pragma: no cover
    _ZERO_COPY = {}
