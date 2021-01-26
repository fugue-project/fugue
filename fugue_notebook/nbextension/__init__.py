def _jupyter_nbextension_paths():
    return [
        {
            "section": "notebook",
            "src": "nbextension",
            "dest": "fugue_notebook",
            "require": "fugue_notebook/main",
        }
    ]
