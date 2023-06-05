from typing import Any, Dict, Tuple, Optional

from triad.utils.convert import get_caller_global_local_vars

from fugue.dataframe import AnyDataFrame
from fugue.exceptions import FugueSQLError
from fugue.execution import AnyExecutionEngine
from fugue.execution.api import get_current_conf

from ..constants import (
    FUGUE_CONF_SQL_IGNORE_CASE,
    FUGUE_CONF_SQL_DIALECT,
    FUGUE_SQL_DEFAULT_DIALECT,
)
from .workflow import FugueSQLWorkflow


def fugue_sql(
    query: str,
    *args: Any,
    fsql_ignore_case: Optional[bool] = None,
    fsql_dialect: Optional[str] = None,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
    **kwargs: Any,
) -> AnyDataFrame:
    """Simplified Fugue SQL interface. This function can still take multiple dataframe
    inputs but will always return the last generated dataframe in the SQL workflow. And
    ``YIELD`` should NOT be used with this function. If you want to use Fugue SQL to
    represent the full workflow, or want to see more Fugue SQL examples,
    please read :func:`~.fugue_sql_flow`.

    :param query: the Fugue SQL string (can be a jinja template)
    :param args: variables related to the SQL string
    :param fsql_ignore_case: whether to ignore case when parsing the SQL string,
        defaults to None (it depends on the engine/global config).
    :param fsql_dialect: the dialect of this fsql,
        defaults to None (it depends on the engine/global config).
    :param kwargs: variables related to the SQL string
    :param engine: an engine like object, defaults to None
    :param engine_conf: the configs for the engine, defaults to None
    :param as_fugue: whether to force return a Fugue DataFrame, defaults to False
    :param as_local: whether return a local dataframe, defaults to False

    :return: the result dataframe

    .. note::

        This function is different from :func:`~fugue.api.raw_sql` which directly
        sends the query to the execution engine to run. This function parses the query
        based on Fugue SQL syntax, creates a
        :class:`~fugue.sql.workflow.FugueSQLWorkflow` which
        could contain multiple raw SQLs plus other operations, and runs and returns
        the last dataframe generated in the workflow.

        This function allows you to parameterize the SQL in a more elegant way. The
        data tables referred in the query can either be automatically extracted from the
        local variables or be specified in the arguments.

    .. caution::

        Currently, we have not unified the dialects of different SQL backends. So there
        can be some slight syntax differences when you switch between backends.
        In addition, we have not unified the UDFs cross different backends, so you
        should be careful to use uncommon UDFs belonging to a certain backend.

        That being said, if you keep your SQL part general and leverage Fugue extensions
        (transformer, creator, processor, outputter, etc.) appropriately, it should be
        easy to write backend agnostic Fugue SQL.

        We are working on unifying the dialects of different SQLs, it should be
        available in the future releases. Regarding unifying UDFs, the effort is still
        unclear.

    .. code-block:: python

        import pandas as pd
        import fugue.api as fa

        def tr(df:pd.DataFrame) -> pd.DataFrame:
            return df.assign(c=2)

        input = pd.DataFrame([[0,1],[3.4]], columns=["a","b"])

        with fa.engine_context("duckdb"):
            res = fa.fugue_sql('''
            SELECT * FROM input WHERE a<{{x}}
            TRANSFORM USING tr SCHEMA *,c:int
            ''', x=2)
            assert fa.as_array(res) == [[0,1,2]]
    """

    dag = _build_dag(
        query,
        fsql_ignore_case=fsql_ignore_case,
        fsql_dialect=fsql_dialect,
        args=args,
        kwargs=kwargs,
    )
    if dag.last_df is not None:
        dag.last_df.yield_dataframe_as("result", as_local=as_local)
    else:  # pragma: no cover
        # impossible case
        raise FugueSQLError(f"no dataframe to output from\n{query}")
    res = dag.run(engine, engine_conf)
    return res["result"] if as_fugue else res["result"].native_as_df()


def fugue_sql_flow(
    query: str,
    *args: Any,
    fsql_ignore_case: Optional[bool] = None,
    fsql_dialect: Optional[str] = None,
    **kwargs: Any,
) -> FugueSQLWorkflow:
    """Fugue SQL full functional interface. This function allows full workflow
    definition using Fugue SQL, and it allows multiple outputs using ``YIELD``.

    :param query: the Fugue SQL string (can be a jinja template)
    :param args: variables related to the SQL string
    :param fsql_ignore_case: whether to ignore case when parsing the SQL string,
        defaults to None (it depends on the engine/global config).
    :param fsql_dialect: the dialect of this fsql,
        defaults to None (it depends on the engine/global config).
    :param kwargs: variables related to the SQL string
    :return: the translated Fugue workflow

    .. note::

        This function is different from :func:`~fugue.api.raw_sql` which directly
        sends the query to the execution engine to run. This function parses the query
        based on Fugue SQL syntax, creates a
        :class:`~fugue.sql.workflow.FugueSQLWorkflow` which
        could contain multiple raw SQLs plus other operations, and runs and returns
        the last dataframe generated in the workflow.

        This function allows you to parameterize the SQL in a more elegant way. The
        data tables referred in the query can either be automatically extracted from the
        local variables or be specified in the arguments.

    .. caution::

        Currently, we have not unified the dialects of different SQL backends. So there
        can be some slight syntax differences when you switch between backends.
        In addition, we have not unified the UDFs cross different backends, so you
        should be careful to use uncommon UDFs belonging to a certain backend.

        That being said, if you keep your SQL part general and leverage Fugue extensions
        (transformer, creator, processor, outputter, etc.) appropriately, it should be
        easy to write backend agnostic Fugue SQL.

        We are working on unifying the dialects of different SQLs, it should be
        available in the future releases. Regarding unifying UDFs, the effort is still
        unclear.

    .. code-block:: python

        import fugue.api.fugue_sql_flow as fsql
        import fugue.api as fa

        # Basic case
        fsql('''
        CREATE [[0]] SCHEMA a:int
        PRINT
        ''').run()

        # With external data sources
        df = pd.DataFrame([[0],[1]], columns=["a"])
        fsql('''
        SELECT * FROM df WHERE a=0
        PRINT
        ''').run()

        # With external variables
        df = pd.DataFrame([[0],[1]], columns=["a"])
        t = 1
        fsql('''
        SELECT * FROM df WHERE a={{t}}
        PRINT
        ''').run()

        # The following is the explicit way to specify variables and datafrems
        # (recommended)
        df = pd.DataFrame([[0],[1]], columns=["a"])
        t = 1
        fsql('''
        SELECT * FROM df WHERE a={{t}}
        PRINT
        ''', df=df, t=t).run()

        # Using extensions
        def dummy(df:pd.DataFrame) -> pd.DataFrame:
            return df

        fsql('''
        CREATE [[0]] SCHEMA a:int
        TRANSFORM USING dummy SCHEMA *
        PRINT
        ''').run()

        # It's recommended to provide full path of the extension inside
        # Fugue SQL, so the SQL definition and exeuction can be more
        # independent from the extension definition.

        # Run with different execution engines
        sql = '''
        CREATE [[0]] SCHEMA a:int
        TRANSFORM USING dummy SCHEMA *
        PRINT
        '''

        fsql(sql).run(spark_session)
        fsql(sql).run("dask")

        with fa.engine_context("duckdb"):
            fsql(sql).run()

        # Passing dataframes between fsql calls
        result = fsql('''
        CREATE [[0]] SCHEMA a:int
        YIELD DATAFRAME AS x

        CREATE [[1]] SCHEMA a:int
        YIELD DATAFRAME AS y
        ''').run(DaskExecutionEngine)

        fsql('''
        SELECT * FROM x
        UNION
        SELECT * FROM y
        UNION
        SELECT * FROM z

        PRINT
        ''', result, z=pd.DataFrame([[2]], columns=["z"])).run()

        # Get framework native dataframes
        result["x"].native  # Dask dataframe
        result["y"].native  # Dask dataframe
        result["x"].as_pandas()  # Pandas dataframe

        # Use lower case fugue sql
        df = pd.DataFrame([[0],[1]], columns=["a"])
        t = 1
        fsql('''
        select * from df where a={{t}}
        print
        ''', df=df, t=t, fsql_ignore_case=True).run()
    """
    dag = _build_dag(
        query,
        fsql_ignore_case=fsql_ignore_case,
        fsql_dialect=fsql_dialect,
        args=args,
        kwargs=kwargs,
    )
    return dag


def _build_dag(
    query: str,
    fsql_ignore_case: Optional[bool],
    fsql_dialect: Optional[str],
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    level: int = -2,
) -> FugueSQLWorkflow:
    global_vars, local_vars = get_caller_global_local_vars(start=level, end=level)
    if fsql_ignore_case is None:
        fsql_ignore_case = get_current_conf().get(FUGUE_CONF_SQL_IGNORE_CASE, False)
    if fsql_dialect is None:
        fsql_dialect = get_current_conf().get(
            FUGUE_CONF_SQL_DIALECT, FUGUE_SQL_DEFAULT_DIALECT
        )
    dag = FugueSQLWorkflow(
        compile_conf={
            FUGUE_CONF_SQL_IGNORE_CASE: fsql_ignore_case,
            FUGUE_CONF_SQL_DIALECT: fsql_dialect,
        }
    )
    try:
        dag._sql(query, global_vars, local_vars, *args, **kwargs)
    except SyntaxError as ex:
        raise SyntaxError(str(ex)).with_traceback(None) from None
    return dag
