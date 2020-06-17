from fugue.extensions.outputter import Outputter
from fugue.dataframe import DataFrames
from fugue.dataframe.utils import _df_eq as df_eq
from threading import RLock


class Show(Outputter):
    LOCK = RLock()

    def process(self, dfs: DataFrames) -> None:
        # TODO: how do we make sure multiple dfs are printed together?
        title = self.params.get_or_none("title", object)
        title = str(title) if title is not None else None
        rows = self.params.get("rows", 10)
        show_count = self.params.get("show_count", False)
        df_arr = list(dfs.values())
        heads = [df.head(rows) for df in df_arr]
        counts = [df.count() if show_count else -1 for df in df_arr]
        with Show.LOCK:
            if title is not None:
                print(title)
            for df, head, count in zip(df_arr, heads, counts):
                df._show(head_rows=head, rows=rows, count=count, title=None)


class AssertEqual(Outputter):
    def process(self, dfs: DataFrames) -> None:
        assert len(dfs) > 1
        expected = dfs[0]
        for i in range(1, len(dfs)):
            df_eq(expected, dfs[i], throw=True, **self.params)


class Save(Outputter):
    def process(self, dfs: DataFrames) -> None:
        assert len(dfs) == 1
        kwargs = self.params.get("params", dict())
        path = self.params.get_or_throw("path", str)
        format_hint = self.params.get("fmt", "")
        mode = self.params.get("mode", "overwrite")
        partition_spec = self.partition_spec
        force_single = self.params.get("single", False)

        self.execution_engine.save_df(
            df=dfs[0],
            path=path,
            format_hint=format_hint,
            mode=mode,
            partition_spec=partition_spec,
            force_single=force_single,
            **kwargs
        )
