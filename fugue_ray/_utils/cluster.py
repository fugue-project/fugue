from fugue import ExecutionEngine

from .._constants import FUGUE_RAY_CONF_SHUFFLE_PARTITIONS, FUGUE_RAY_DEFAULT_PARTITIONS
from fugue.constants import FUGUE_CONF_DEFAULT_PARTITIONS


def get_default_partitions(engine: ExecutionEngine) -> int:
    n = engine.conf.get(
        FUGUE_RAY_DEFAULT_PARTITIONS, engine.conf.get(FUGUE_CONF_DEFAULT_PARTITIONS, -1)
    )
    return n if n >= 0 else engine.get_current_parallelism() * 2


def get_default_shuffle_partitions(engine: ExecutionEngine) -> int:
    n = engine.conf.get(FUGUE_RAY_CONF_SHUFFLE_PARTITIONS, -1)
    return n if n >= 0 else get_default_partitions(engine)
