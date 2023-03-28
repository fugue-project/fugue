from typing import Any, Callable, Optional

from triad import Schema, assert_or_throw, to_uuid

from fugue.collections.yielded import Yielded
from fugue.dataframe import DataFrame
from fugue.exceptions import FugueWorkflowCompileError
from fugue.execution.api import as_fugue_engine_df
from fugue.extensions.creator import Creator


class Load(Creator):
    def create(self) -> DataFrame:
        kwargs = self.params.get("params", dict())
        path = self.params.get_or_throw("path", str)
        format_hint = self.params.get("fmt", "")
        columns = self.params.get_or_none("columns", object)

        return self.execution_engine.load_df(
            path=path, format_hint=format_hint, columns=columns, **kwargs
        )


class CreateData(Creator):
    def __init__(
        self,
        df: Any,
        schema: Any = None,
        data_determiner: Optional[Callable[[Any], Any]] = None,
    ) -> None:
        if isinstance(df, Yielded):
            assert_or_throw(
                schema is None,
                FugueWorkflowCompileError("schema must be None when data is Yielded"),
            )
        super().__init__()
        self._df = df
        self._schema = schema if schema is None else Schema(schema)
        self._data_determiner = data_determiner

    def create(self) -> DataFrame:
        if isinstance(self._df, Yielded):
            return self.execution_engine.load_yielded(self._df)
        return as_fugue_engine_df(self.execution_engine, self._df, schema=self._schema)

    def _df_uid(self):
        if self._data_determiner is not None:
            return self._data_determiner(self._df)
        if isinstance(self._df, Yielded):
            return self._df
        return 1

    def __uuid__(self) -> str:
        return to_uuid(super().__uuid__(), self._df_uid(), self._schema)
