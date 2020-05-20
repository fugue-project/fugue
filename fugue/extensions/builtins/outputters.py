from fugue.extensions.outputter import Outputter
from fugue.dataframe import DataFrames
from fugue.dataframe.utils import _df_eq as df_eq


class Show(Outputter):
    def process(self, dfs: DataFrames) -> None:
        # TODO: how do we make sure multiple dfs are printed together?
        for df in dfs.values():
            df.show(
                self.params.get("rows", 10),
                self.params.get("count", False),
                title=self.params.get("title", ""),
            )


class AssertEqual(Outputter):
    def process(self, dfs: DataFrames) -> None:
        assert len(dfs) > 1
        expected = dfs[0]
        for i in range(1, len(dfs)):
            df_eq(expected, dfs[i], throw=True, **self.params)
