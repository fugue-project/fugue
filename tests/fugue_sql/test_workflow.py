from fugue.dataframe import DataFrame
from fugue.dataframe.utils import _df_eq
from fugue.execution.native_execution_engine import NativeExecutionEngine
from fugue.workflow.workflow import WorkflowDataFrame
from fugue_sql.exceptions import FugueSQLError, FugueSQLSyntaxError
from fugue_sql.workflow import FugueSQLWorkflow
from pytest import raises


def test_workflow_conf():
    dag = FugueSQLWorkflow(NativeExecutionEngine(
        {"x": 10, "fugue.sql.compile.simple_assign": "false"}))
    assert 10 == dag.conf.get_or_throw("x", int)
    assert not dag.conf.get_or_throw("fugue.sql.compile.simple_assign", bool)
    assert not dag.conf.get_or_throw("fugue.sql.compile.ignore_case", bool)


def test_conf_override():
    with raises(FugueSQLSyntaxError):
        FugueSQLWorkflow()("create [[0]] schema a:int")
    with FugueSQLWorkflow(NativeExecutionEngine(
            {"fugue.sql.compile.ignore_case": "true"})) as dag:
        a = dag.df([[0], [1]], "a:int")
        dag("""
        b = create [[0],[1]] schema a:int
        output a,b using assert_eq""")


def test_show():
    with FugueSQLWorkflow() as dag:
        dag("""
        a = CREATE[[0], [1]] SCHEMA a: int
        b = CREATE[[0], [1]] SCHEMA a: int
        PRINT a, b ROWS 10 ROWCOUNT TITLE "abc"
        PRINT a, b
        """)


def test_use_df():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        dag("""
        b=CREATE[[0], [1]] SCHEMA a: int
        OUTPUT a, b USING assert_eq
        """)
        dag["b"].assert_eq(a)


def test_use_param():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        x = 0
        dag("""
        b=CREATE[[{{x}}], [{{y}}]] SCHEMA a: int
        OUTPUT a, b USING assert_eq
        """, y=1)


def test_multiple_sql():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        dag("b = CREATE [[0],[1]] SCHEMA a:int")
        dag("OUTPUT a,b USING assert_eq")
        assert dag["a"] is a
        assert isinstance(dag["b"], WorkflowDataFrame)


def test_multiple_sql_with_reset():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        dag("b = CREATE [[0],[1]] SCHEMA a:int")
        a = dag.df([[0], [2]], "a:int")
        b = dag.df([[0], [2]], "a:int")
        dag("""
        OUTPUT a, b USING assert_eq
        OUTPUT a, (CREATE[[0], [2]] SCHEMA a: int) USING assert_eq
        """)


def test_multiple_blocks():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        c = 1
        dag("""
        OUTPUT a, (CREATE[[0], [1]] SCHEMA a: int) USING assert_eq
        """)
    # dataframe can't pass to another workflow
    with FugueSQLWorkflow() as dag:
        assert "a" in locals()
        with raises(FugueSQLError):
            dag("""
            OUTPUT a, (CREATE[[0], [1]] SCHEMA a: int) USING assert_eq
            """)
    # other local variables are fine
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        dag("""
        OUTPUT a, (CREATE[[0], [{{c}}]] SCHEMA a: int) USING assert_eq
        """)


def test_function_calls():
    with FugueSQLWorkflow() as dag:
        a = dag.df([[0], [1]], "a:int")
        _eq(dag, a)


def _eq(dag, a):
    dag("""
    OUTPUT a, (CREATE[[0], [1]] SCHEMA a: int) USING assert_eq
    """)


def assert_eq(df1: DataFrame, df2: DataFrame) -> None:
    _df_eq(df1, df2, throw=True)
