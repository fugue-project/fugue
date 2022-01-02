from builtins import isinstance
from typing import Any, Dict, Tuple

from fugue import (
    DataFrame,
    FugueWorkflow,
    WorkflowDataFrame,
    WorkflowDataFrames,
    Yielded,
)
from fugue.constants import FUGUE_CONF_SQL_IGNORE_CASE
from fugue.workflow import is_acceptable_raw_df
from fugue_sql._parse import FugueSQL
from fugue_sql._utils import LazyWorkflowDataFrame, fill_sql_template
from fugue_sql._visitors import FugueSQLHooks, _Extensions
from fugue_sql.exceptions import FugueSQLSyntaxError
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import get_caller_global_local_vars


class FugueSQLWorkflow(FugueWorkflow):
    """Fugue workflow that supports Fugue SQL. Please read |FugueSQLTutorial|."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._sql_vars: Dict[str, WorkflowDataFrame] = {}

    @property
    def sql_vars(self) -> Dict[str, WorkflowDataFrame]:
        return self._sql_vars

    def __call__(self, code: str, *args: Any, **kwargs: Any) -> None:
        global_vars, local_vars = get_caller_global_local_vars()
        variables = self._sql(
            code, self._sql_vars, global_vars, local_vars, *args, **kwargs
        )
        for k, v in variables.items():
            if isinstance(v, WorkflowDataFrame) and v.workflow is self:
                self._sql_vars[k] = v

    def _sql(
        self, code: str, *args: Any, **kwargs: Any
    ) -> Dict[str, Tuple[WorkflowDataFrame, WorkflowDataFrames, LazyWorkflowDataFrame]]:
        # TODO: move dict construction to triad
        params: Dict[str, Any] = {}
        for a in args:
            assert_or_throw(
                isinstance(a, Dict), lambda: f"args can only have dict: {a}"
            )
            params.update(a)
        params.update(kwargs)
        params, dfs = self._split_params(params)
        code = fill_sql_template(code, params)
        sql = FugueSQL(
            code,
            "fugueLanguage",
            ignore_case=self.conf.get_or_throw(FUGUE_CONF_SQL_IGNORE_CASE, bool),
            simple_assign=True,
        )
        v = _Extensions(
            sql, FugueSQLHooks(), self, dfs, local_vars=params  # type: ignore
        )
        v.visit(sql.tree)
        return v.variables

    def _split_params(
        self, params: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], Dict[str, LazyWorkflowDataFrame]]:
        p: Dict[str, Any] = {}
        dfs: Dict[str, LazyWorkflowDataFrame] = {}
        for k, v in params.items():
            if isinstance(v, (int, str, float, bool)):
                p[k] = v
            elif isinstance(v, (DataFrame, Yielded)) or is_acceptable_raw_df(v):
                dfs[k] = LazyWorkflowDataFrame(k, v, self)
            else:
                p[k] = v
        return p, dfs


def fsql(
    sql: str, *args: Any, fsql_ignore_case: bool = False, **kwargs: Any
) -> FugueSQLWorkflow:
    """Fugue SQL functional interface

    :param sql: the Fugue SQL string (can be a jinja template)
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
    global_vars, local_vars = get_caller_global_local_vars()
    dag = FugueSQLWorkflow(None, {FUGUE_CONF_SQL_IGNORE_CASE: fsql_ignore_case})
    try:
        dag._sql(sql, global_vars, local_vars, *args, **kwargs)
    except FugueSQLSyntaxError as ex:
        raise FugueSQLSyntaxError(str(ex)).with_traceback(None) from None
    return dag
