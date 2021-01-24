import os

import pandas as pd
from fugue.dataframe import DataFrame
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.utils import _df_eq
from fugue.execution.native_execution_engine import NativeExecutionEngine
from fugue.extensions._builtins.outputters import Show
from fugue.workflow.workflow import WorkflowDataFrame
from pytest import raises

from fugue_sql import FugueSQLWorkflow, fsql
from fugue_sql.exceptions import FugueSQLError, FugueSQLSyntaxError


def test_workflow_conf():
    dag = FugueSQLWorkflow(
        NativeExecutionEngine({"x": 10, "fugue.sql.compile.simple_assign": "false"})
    )
    assert 10 == dag.conf.get_or_throw("x", int)
    assert not dag.conf.get_or_throw("fugue.sql.compile.simple_assign", bool)
    assert not dag.conf.get_or_throw("fugue.sql.compile.ignore_case", bool)


def test_conf_override():
    with raises(FugueSQLSyntaxError):
        FugueSQLWorkflow()("create [[0]] schema a:int")
    with FugueSQLWorkflow(
        NativeExecutionEngine({"fugue.sql.compile.ignore_case": "true"})
    ) as dag:
        a = dag.df([[0], [1]], "a:int")
        dag(
            """
        b = create [[0],[1]] schema a:int
        output a,b using assert_eq"""
        )


def test_show():
    class _CustomShow(object):
        def __init__(self):
            self.called = False

        def show(self, schema, head_rows, title, rows, count):
            print(schema, head_rows)
            print(title, rows, count)
            self.called = True

    cs = _CustomShow()
    with FugueSQLWorkflow() as dag:
        dag(
            """
        a = CREATE[[0], [1]] SCHEMA a: int
        b = CREATE[[0], [1]] SCHEMA a: int
        PRINT a, b ROWS 10 ROWCOUNT TITLE "abc"
        PRINT a, b
        """
        )
    assert not cs.called
    Show.set_hook(cs.show)
    with FugueSQLWorkflow() as dag:
        dag(
            """
        a = CREATE[[0], [1]] SCHEMA a: int
        b = CREATE[[0], [1]] SCHEMA a: int
        PRINT a, b ROWS 10 ROWCOUNT TITLE "abc"
        PRINT a, b
        """
        )

    assert cs.called


def test_jinja_keyword_in_sql():
    with FugueSQLWorkflow() as dag:
        dag("""{% raw -%}
            CREATE [["{%'{%'"]] SCHEMA a:str
            SELECT * WHERE a LIKE '{%'
            PRINT
            {%- endraw %}""")
    
        df = dag.df([["b"]], "a:str")
        x = "b"
        dag("""
        df2 = SELECT *
        FROM df
        WHERE a = "{{x}}"
        OUTPUT df, df2 USING assert_eq
        """)


def test_use_df(tmpdir):
    # df generated inside dag
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        dag(
            """
        b=CREATE[[0], [1]] SCHEMA a: int
        OUTPUT a, b USING assert_eq
        """
        )
        dag.sql_vars["b"].assert_eq(a)

    # external non-workflowdataframe
    arr = ArrayDataFrame([[0], [1]], "a:int")
    with FugueSQLWorkflow() as dag:
        dag(
            """
        b=CREATE[[0], [1]] SCHEMA a: int
        OUTPUT a, b USING assert_eq
        """,
            a=arr,
        )
        dag.sql_vars["b"].assert_eq(dag.df([[0], [1]], "a:int"))

    # from yield
    engine = NativeExecutionEngine(
        conf={"fugue.workflow.checkpoint.path": os.path.join(tmpdir, "ck")}
    )
    with FugueSQLWorkflow(engine) as dag:
        dag("CREATE[[0], [1]] SCHEMA a: int YIELD AS b")
        res = dag.yields["b"]

    with FugueSQLWorkflow(engine) as dag:
        dag(
            """
        b=CREATE[[0], [1]] SCHEMA a: int
        OUTPUT a, b USING assert_eq
        """,
            a=res,
        )


def test_lazy_use_df():
    df1 = pd.DataFrame([[0]], columns=["a"])
    df2 = pd.DataFrame([[1]], columns=["a"])
    # although df2 is defined as a local variable
    # since it is not used in dag1, so it was never converted

    dag1 = FugueSQLWorkflow()
    dag1("""PRINT df1""")

    dag2 = FugueSQLWorkflow()
    dag2.df(df1).show()

    assert dag1.spec_uuid() == dag2.spec_uuid()


def test_use_param():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        x = 0
        dag(
            """
        b=CREATE[[{{x}}], [{{y}}]] SCHEMA a: int
        OUTPUT a, b USING assert_eq
        """,
            y=1,
        )


def test_multiple_sql():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        dag("b = CREATE [[0],[1]] SCHEMA a:int")
        dag("OUTPUT a,b USING assert_eq")


def test_multiple_sql_with_reset():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        dag("b = CREATE [[0],[1]] SCHEMA a:int")
        a = dag.df([[0], [2]], "a:int")
        b = dag.df([[0], [2]], "a:int")
        dag(
            """
        OUTPUT a, b USING assert_eq
        OUTPUT a, (CREATE[[0], [2]] SCHEMA a: int) USING assert_eq
        """
        )


def test_multiple_blocks():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        c = 1
        dag(
            """
        OUTPUT a, (CREATE[[0], [1]] SCHEMA a: int) USING assert_eq
        """
        )
    # dataframe can't pass to another workflow
    with FugueSQLWorkflow() as dag:
        assert "a" in locals()
        with raises(FugueSQLError):
            dag(
                """
            OUTPUT a, (CREATE[[0], [1]] SCHEMA a: int) USING assert_eq
            """
            )
    # other local variables are fine
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        dag(
            """
        OUTPUT a, (CREATE[[0], [{{c}}]] SCHEMA a: int) USING assert_eq
        """
        )


def test_function_calls():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        _eq(dag, a)


def test_local_instance_as_extension():
    class _Mock(object):
        # schema: *
        def t(self, df: pd.DataFrame) -> pd.DataFrame:
            return df

        def test(self):
            with FugueSQLWorkflow() as dag:
                dag(
                    """
                a = CREATE [[0],[1]] SCHEMA a:int
                b = TRANSFORM USING self.t
                OUTPUT a,b USING assert_eq
                """
                )

    m = _Mock()
    m.test()
    with FugueSQLWorkflow() as dag:
        dag(
            """
        a = CREATE [[0],[1]] SCHEMA a:int
        b = TRANSFORM USING m.t
        OUTPUT a,b USING assert_eq
        """
        )


def test_call_back():
    class CB(object):
        def __init__(self):
            self.n = 0

        def incr(self, n):
            self.n += n
            return self.n

    cb = CB()

    # schema: *
    def t(df: pd.DataFrame, incr: callable) -> pd.DataFrame:
        incr(1)
        return df

    with FugueSQLWorkflow() as dag:
        dag(
            """
        a = CREATE [[0],[1],[1]] SCHEMA a:int
        TRANSFORM PREPARTITION BY a USING t CALLBACK cb.incr PERSIST
        OUTTRANSFORM a PREPARTITION BY a USING t CALLBACK cb.incr
        """
        )

    assert 4 == cb.n


def test_fsql():
    # schema: *,x:long
    def t(df: pd.DataFrame) -> pd.DataFrame:
        df["x"] = 1
        return df

    df = pd.DataFrame([[0], [1]], columns=["a"])
    result = fsql(
        """
    SELECT * FROM df WHERE a>{{p}}
    UNION ALL
    SELECT * FROM df2 WHERE a>{{p}}
    result = TRANSFORM USING t
    """,
        df2=pd.DataFrame([[0], [1]], columns=["a"]),
        p=0,
    ).run()
    assert [[1, 1], [1, 1]] == result["result"].as_array()


def _eq(dag, a):
    dag(
        """
    OUTPUT a, (CREATE[[0], [1]] SCHEMA a: int) USING assert_eq
    """
    )


def assert_eq(df1: DataFrame, df2: DataFrame) -> None:
    _df_eq(df1, df2, throw=True)
