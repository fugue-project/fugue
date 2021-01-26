from typing import Any

from fugue_notebook.env import setup_fugue_notebook


def load_ipython_extension(ip: Any) -> None:
    setup_fugue_notebook(ip, None)


def _jupyter_nbextension_paths():
    return [
        {
            "section": "notebook",
            "src": "nbextension",
            "dest": "fugue_notebook",
            "require": "fugue_notebook/main",
        }
    ]
