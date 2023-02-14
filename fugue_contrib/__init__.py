import importlib
from typing import Any

from triad.utils.dispatcher import ConditionalDispatcher

from .contrib import FUGUE_CONTRIB


def load_domain(domain: str) -> None:
    path = FUGUE_CONTRIB[domain]["module"]
    importlib.import_module(path)


def dispatch_domain_func(
    domain: str, dispatcher: ConditionalDispatcher, *args: Any, **kwargs: Any
) -> Any:

    for f in dispatcher._funcs:
        if dispatcher._match(f[2], *args, **kwargs):
            return f[3](*args, **kwargs)
    raise ValueError(f"{domain} is not registered")
