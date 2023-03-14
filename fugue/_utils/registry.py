from typing import Callable

from triad import conditional_dispatcher
from triad.utils.dispatcher import ConditionalDispatcher

from ..constants import FUGUE_ENTRYPOINT


def fugue_plugin(func: Callable) -> ConditionalDispatcher:
    return conditional_dispatcher(entry_point=FUGUE_ENTRYPOINT)(func)  # type: ignore
