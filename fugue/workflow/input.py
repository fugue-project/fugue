from typing import Type


def register_raw_df_type(df_type: Type) -> None:  # pragma: no cover
    """TODO: This function is to be removed before 0.9.0

    .. deprecated:: 3.1
        Register using :func:`fugue.api.is_df` instead.
    """
    raise DeprecationWarning("use fugue.api.is_df to register the dataframe")
