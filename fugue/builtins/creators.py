from fugue.creator import Creator
from fugue.dataframe import DataFrame
from triad.collections import Schema


class CreateData(Creator):
    def create(self) -> DataFrame:
        return self.execution_engine.to_df(
            self.params.get_or_throw("data", object),
            self.params.get_or_none("schema", Schema),
        )
