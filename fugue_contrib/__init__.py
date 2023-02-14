import importlib

from .contrib import FUGUE_CONTRIB


def load_domain(domain: str) -> None:
    if domain in FUGUE_CONTRIB:
        path = FUGUE_CONTRIB[domain]["module"]
        importlib.import_module(path)
