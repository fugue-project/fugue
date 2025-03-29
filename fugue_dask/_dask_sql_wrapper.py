from typing import Any, Optional

import dask.dataframe as dd

try:
    from dask.dataframe.dask_expr.io.parquet import ReadParquet

    HAS_DASK_EXPR = True  # newer dask
except ImportError:  # pragma: no cover
    HAS_DASK_EXPR = False  # older dask

if not HAS_DASK_EXPR:  # pragma: no cover
    try:
        from dask_sql import Context as ContextWrapper  # pylint: disable-all
    except ImportError:  # pragma: no cover
        raise ImportError(
            "dask-sql is not installed. Please install it with `pip install dask-sql`"
        )
else:
    from triad.utils.assertion import assert_or_throw

    try:
        from dask_sql import Context
        from dask_sql.datacontainer import Statistics
        from dask_sql.input_utils import InputUtil
    except ImportError:  # pragma: no cover
        raise ImportError(
            "dask-sql is not installed. Please install it with `pip install dask-sql`"
        )

    class ContextWrapper(Context):  # type: ignore
        def create_table(
            self,
            table_name: str,
            input_table: dd.DataFrame,
            format: Optional[str] = None,  # noqa
            persist: bool = False,
            schema_name: Optional[str] = None,
            statistics: Optional[Statistics] = None,
            gpu: bool = False,
            **kwargs: Any,
        ) -> None:  # pragma: no cover
            assert_or_throw(
                isinstance(input_table, dd.DataFrame),
                lambda: ValueError(
                    f"input_table must be a dask dataframe, but got {type(input_table)}"
                ),
            )
            assert_or_throw(
                dd._dask_expr_enabled(), lambda: ValueError("Dask expr must be enabled")
            )
            schema_name = schema_name or self.schema_name

            dc = InputUtil.to_dc(
                input_table,
                table_name=table_name,
                format=format,
                persist=persist,
                gpu=gpu,
                **kwargs,
            )

            dask_filepath = None
            operations = input_table.find_operations(ReadParquet)
            for op in operations:
                dask_filepath = op._args[0]

            dc.filepath = dask_filepath
            self.schema[schema_name].filepaths[table_name.lower()] = dask_filepath

            if not statistics:
                statistics = Statistics(float("nan"))
            dc.statistics = statistics

            self.schema[schema_name].tables[table_name.lower()] = dc
            self.schema[schema_name].statistics[table_name.lower()] = statistics
