from typing import Any, Set, Type

import pandas as pd
from fugue.dataframe import DataFrame

_VALID_RAW_DF_TYPES: Set[Type] = set([pd.DataFrame, DataFrame])


def register_raw_df_type(df_type: Type) -> None:
    """Register a base type of dataframe that can be recognized by
    :class:`~fugue.workflow.workflow.FugueWorkflow` and converted to
    :class:`~fugue.workflow.workflow.WorkflowDataFrame`

    :param df_type: dataframe type, for example ``dask.dataframe.DataFrame``
    """
    _VALID_RAW_DF_TYPES.add(df_type)


def is_acceptable_raw_df(df: Any) -> bool:
    """Whether the input ``df`` can be converted to
    :class:`~fugue.workflow.workflow.WorkflowDataFrame`

    :param df: input raw dataframe
    :return: whether this dataframe is convertible
    """
    return any(isinstance(df, t) for t in _VALID_RAW_DF_TYPES)
