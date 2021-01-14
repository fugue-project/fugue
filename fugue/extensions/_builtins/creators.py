from fugue.extensions.creator import Creator
from fugue.dataframe import DataFrame
from fugue.collections.yielded import Yielded


class Load(Creator):
    def create(self) -> DataFrame:
        kwargs = self.params.get("params", dict())
        path = self.params.get_or_throw("path", str)
        format_hint = self.params.get("fmt", "")
        columns = self.params.get_or_none("columns", object)

        return self.execution_engine.load_df(
            path=path, format_hint=format_hint, columns=columns, **kwargs
        )


class LoadYielded(Creator):
    def create(self) -> DataFrame:
        yielded = self.params.get_or_throw("yielded", Yielded)
        return self.execution_engine.load_df(path=yielded.path)
