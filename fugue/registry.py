import inspect
from typing import Any, Optional

import pyarrow as pa

from fugue._utils.interfaceless import (
    DataFrameParam,
    SimpleAnnotationConverter,
    register_annotation_converter,
)
from fugue.dataframe import ArrowDataFrame, DataFrame
from fugue.workflow import register_raw_df_type


def _register_extra() -> None:
    """Register Fugue core additional types

    .. note::

        This function is automatically called when you do

        >>> import fugue
    """
    _register_raw_dataframes()
    _register_engines()
    _register_annotation_converters()


def _register_raw_dataframes() -> None:
    register_raw_df_type(pa.Table)


def _register_engines() -> None:
    pass


def _register_annotation_converters() -> None:
    register_annotation_converter(
        0.8,
        SimpleAnnotationConverter(
            pa.Table,
            lambda param: _PyArrowTableParam(param),
        ),
    )


class _PyArrowTableParam(DataFrameParam):
    def __init__(self, param: Optional[inspect.Parameter]):
        super().__init__(param, annotation="Table")

    def to_input_data(self, df: DataFrame, ctx: Any) -> Any:
        return df.as_arrow()

    def to_output_df(self, output: Any, schema: Any, ctx: Any) -> DataFrame:
        assert isinstance(output, pa.Table)
        return ArrowDataFrame(output, schema=schema)

    def count(self, df: Any) -> int:  # pragma: no cover
        return df.count()
