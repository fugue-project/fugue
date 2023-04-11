from fugue.duckdb.registry import _register_engines


def _register() -> None:
    """Register Fugue core additional types

    .. note::

        This function is automatically called when you do

        >>> import fugue
    """
    _register_engines()
