from typing import Any, Set, Type

from fugue.extensions._builtins import CreateData
from fugue.extensions.creator import parse_creator

_VALID_RAW_DF_TYPES: Set[Type] = set()


def register_raw_df_type(df_type: Type) -> None:
    """Register a base type of dataframe that can be recognized by
    :class:`~fugue.workflow.workflow.FugueWorkflow` and converted to
    :class:`~fugue.workflow.workflow.WorkflowDataFrame`

    :param df_type: dataframe type, for example ``dask.dataframe.DataFrame``
    """

    _VALID_RAW_DF_TYPES.add(df_type)

    @parse_creator.candidate(lambda x: isinstance(x, df_type), priority=0.5)
    def _parse(x: Any) -> Any:
        return CreateData(x)


def is_acceptable_raw_df(df: Any) -> bool:
    """Whether the input ``df`` can be converted to
    :class:`~fugue.workflow.workflow.WorkflowDataFrame`
    :param df: input raw dataframe
    :return: whether this dataframe is convertible
    """
    import fugue._utils.register  # pylint: disable=W0611 # noqa: F401

    return any(isinstance(df, t) for t in _VALID_RAW_DF_TYPES)
