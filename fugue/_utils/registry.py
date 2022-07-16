from typing import Callable
from triad import conditional_dispatcher

_FUGUE_ENTRYPOINT = "fugue.plugins"


def fugue_plugin(func: Callable) -> Callable:
    return conditional_dispatcher(entry_point=_FUGUE_ENTRYPOINT)(func)
