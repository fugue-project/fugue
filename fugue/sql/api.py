from typing import Any, Dict, Tuple

from triad.utils.convert import get_caller_global_local_vars

from fugue.dataframe import DataFrame
from fugue.exceptions import FugueSQLError
from fugue.execution import AnyExecutionEngine
from fugue.workflow.workflow import FugueWorkflowResult

from ..constants import FUGUE_CONF_SQL_IGNORE_CASE
from .workflow import FugueSQLWorkflow


def fugue_sql_flow(
    query: str,
    *args: Any,
    fsql_ignore_case: bool = False,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    **kwargs: Any,
) -> FugueWorkflowResult:
    dag = _build_dag(query, fsql_ignore_case=fsql_ignore_case, args=args, kwargs=kwargs)
    return dag.run(engine, engine_conf)


def fugue_sql(
    query: str,
    *args: Any,
    fsql_ignore_case: bool = False,
    engine: AnyExecutionEngine = None,
    engine_conf: Any = None,
    as_fugue: bool = False,
    as_local: bool = False,
    **kwargs: Any,
) -> DataFrame:
    dag = _build_dag(query, fsql_ignore_case=fsql_ignore_case, args=args, kwargs=kwargs)
    if dag.last_df is not None:
        dag.last_df.yield_dataframe_as("result", as_local=as_local)
    else:  # pragma: no cover
        # impossible case
        raise FugueSQLError(f"no dataframe to output from\n{query}")
    res = dag.run(engine, engine_conf)
    return res["result"] if as_fugue else res["result"].native_as_df()


def fsql(
    query: str, *args: Any, fsql_ignore_case: bool = False, **kwargs: Any
) -> FugueSQLWorkflow:
    """Fugue SQL functional interface

    :param query: the Fugue SQL string (can be a jinja template)
    :param args: variables related to the SQL string
    :param fsql_ignore_case: whether to ignore case when parsing the SQL string
        defaults to False.
    :param kwargs: variables related to the SQL string
    :return: the translated Fugue workflow

    .. code-block:: python

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

        fsql(sql).run(user_defined_spark_session())
        fsql(sql).run(SparkExecutionEngine, {"spark.executor.instances":10})
        fsql(sql).run(DaskExecutionEngine)

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
    dag = _build_dag(query, fsql_ignore_case=fsql_ignore_case, args=args, kwargs=kwargs)
    return dag


def _build_dag(
    query: str,
    fsql_ignore_case: bool,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    level: int = -2,
) -> FugueSQLWorkflow:
    global_vars, local_vars = get_caller_global_local_vars(start=level, end=level)
    dag = FugueSQLWorkflow(compile_conf={FUGUE_CONF_SQL_IGNORE_CASE: fsql_ignore_case})
    try:
        dag._sql(query, global_vars, local_vars, *args, **kwargs)
    except SyntaxError as ex:
        raise SyntaxError(str(ex)).with_traceback(None) from None
    return dag
