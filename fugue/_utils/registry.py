from typing import Callable
from triad import conditional_dispatcher
from triad.utils.dispatcher import ConditionalDispatcher

_FUGUE_ENTRYPOINT = "fugue.plugins"


def fugue_plugin(func: Callable) -> ConditionalDispatcher:
    return conditional_dispatcher(entry_point=_FUGUE_ENTRYPOINT)(func)  # type: ignore
