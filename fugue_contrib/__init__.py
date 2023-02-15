import importlib

from .contrib import FUGUE_CONTRIB


def load_namespace(namespace: str) -> None:
    if namespace in FUGUE_CONTRIB:
        path = FUGUE_CONTRIB[namespace]["module"]
        importlib.import_module(path)
