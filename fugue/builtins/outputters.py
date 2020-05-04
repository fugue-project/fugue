from fugue.outputter import Outputter
from fugue.dataframe import DataFrames


class Show(Outputter):
    def process(self, dfs: DataFrames) -> None:
        # TODO: how do we make sure multiple dfs are printed together?
        for df in dfs.values():
            df.show(
                self.params.get("rows", 10),
                self.params.get("count", False),
                title=self.params.get("title", ""),
            )
