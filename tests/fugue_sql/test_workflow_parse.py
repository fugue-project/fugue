import json
from typing import Any, Iterable, List

from fugue import (
    FugueWorkflow,
    SqliteEngine,
    WorkflowDataFrame,
    WorkflowDataFrames,
    module,
)
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame
from fugue.extensions import OutputTransformer
from fugue.extensions.transformer.convert import _to_output_transformer
from pytest import raises
from triad import to_uuid
from triad.collections.schema import Schema
from triad.utils.convert import get_caller_global_local_vars

from fugue_sql._parse import FugueSQL
from fugue_sql._visitors import FugueSQLHooks, _Extensions, _VisitorBase
from fugue_sql.exceptions import FugueSQLError


def test_create_data():
    w = FugueWorkflow().df([[0], [1]], "a:int", data_determiner=to_uuid)
    assert_eq(
        """
    a=create [[0],[1]] schema a:int
    """,
        w.workflow,
    )


def test_create():
    dag = FugueWorkflow()
    dag.create(mock_create1, params=dict(n=1))
    dag.create(mock_create2, schema="a:int", params=dict(n=1))
    assert_eq(
        """
    a=create using mock_create1 params n:1
    b=create using mock_create2(n=1) schema a:int
    """,
        dag,
    )


def test_process():
    # basic features, nest
    dag = FugueWorkflow()
    a1 = dag.create(mock_create1, params=dict(n=1))
    a2 = dag.create(mock_create1, params=dict(n=2))
    dag.process(a1, a2, using=mock_processor1, params=dict(n=3))
    dag.process(a2, a1, using=mock_processor2, schema="b:int", params=dict(n=4))
    dag.process(
        dag.create(mock_create1, params=dict(n=5)),
        dag.create(mock_create1, params=dict(n=6)),
        using=mock_processor1,
        params=dict(n=7),
    )
    assert_eq(
        """
    a=create using mock_create1 params n:1
    b=create using mock_create1 params n:2
    process a,b using mock_processor1(n=3)
    process b,a using mock_processor2(n=4) schema b:int
    process  # nested
        (create using mock_create1(n=5)),
        (create using mock_create1(n=6))
        using mock_processor1(n=7)
    """,
        dag,
    )

    # anonymous, nested anonymous
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1)).process(mock_processor3)
    b = a.partition(by=["a"]).process(mock_processor3)
    c = a.process(mock_processor3)
    dag.process(b, c, using=mock_processor1)
    assert_eq(
        """
    create using mock_create1 params n:1
    process using mock_processor3
    process  # nested
        (process prepartition by a using mock_processor3),
        (process using mock_processor3)
        using mock_processor1
    """,
        dag,
    )

    # no last dataframe
    with raises(FugueSQLError):
        assert_eq(
            """
        process using mock_processor3
        """,
            None,
        )

    # dict like dataframes
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    b = dag.create(mock_create1, params=dict(n=2))
    dag.process(dict(df1=a, df2=b), using=mock_processor1)
    assert_eq(
        """
    process
        df1=(create using mock_create1(n=1)),
        df2:(create using mock_create1(n=2))
        using mock_processor1
    """,
        dag,
    )


def test_zip():
    dag = FugueWorkflow()
    a1 = dag.create(mock_create1, params=dict(n=1))
    a2 = dag.create(mock_create1, params=dict(n=2))
    a1.zip(a2)
    assert_eq(
        """
    a=create using mock_create1 params n:1
    zip a,(create using mock_create1 params n:2)
    """,
        dag,
    )

    dag = FugueWorkflow()
    a1 = dag.create(mock_create1, params=dict(n=1))
    a2 = dag.create(mock_create1, params=dict(n=2))
    a1.zip(a2, how="left_outer", partition=dict(by=["a"], presort="b DESC"))
    assert_eq(
        """
    a=create using mock_create1 params n:1
    zip a,(create using mock_create1 params n:2) left
        outer by a presort b desc
    """,
        dag,
    )


def test_cotransform():
    dag = FugueWorkflow()
    a1 = dag.create(mock_create1, params=dict(n=1))
    a2 = dag.create(mock_create1, params=dict(n=2))
    z = dag.zip(a1, a2)
    t = z.partition(num=3).transform(mock_cotransformer1, params=dict(n=3))
    assert_eq(
        """
    zip 
        (create using mock_create1 params n:1),
        (create using mock_create1 params n:2)
    transform prepartition 3 using mock_cotransformer1(n=3)
    """,
        dag,
    )


def test_transform():
    w = (
        FugueWorkflow()
        .df([[0], [1]], "a:int", data_determiner=to_uuid)
        .transform(mock_transformer, schema=Schema("a:int"), params=dict(n=2))
    )
    assert_eq(
        """
    create [[0],[1]] schema a:int
    transform using mock_transformer(n=2) schema a:int
    """,
        w.workflow,
    )

    w = (
        FugueWorkflow()
        .df([[0], [1]], "a:int", data_determiner=to_uuid)
        .partition(by=["a"], presort="b DESC", num="ROWCOUNT/2")
        .transform(mock_transformer, schema="*", params=dict(n=2))
    )
    assert_eq(
        """
    create [[0],[1]] schema a:int
    
    transform 
        prepartition ROWCOUNT / 2 by a presort b desc
        using mock_transformer(n=2) schema *
    """,
        w.workflow,
    )

    def _func(a: int, b: int) -> int:
        return a + b

    w = (
        FugueWorkflow()
        .df([[0], [1]], "a:int", data_determiner=to_uuid)
        .partition(by=["a"], presort="b DESC", num="ROWCOUNT/2")
        .transform(mock_transformer, schema="*", params=dict(n=2), callback=_func)
    )
    assert_eq(
        """
    create [[0],[1]] schema a:int
    
    transform 
        prepartition ROWCOUNT / 2 by a presort b desc
        using mock_transformer(n=2) schema *
        callback _func
    """,
        w.workflow,
    )


def test_out_transform():
    class OT(OutputTransformer):
        def process(self, df):
            return

    o = _to_output_transformer(OT)
    w = FugueWorkflow()
    w.df([[0], [1]], "a:int", data_determiner=to_uuid).out_transform(
        o, params=dict(n=2)
    )
    assert_eq(
        """
    create [[0],[1]] schema a:int
    outtransform using OT(n=2)
    """,
        w,
    )

    w = FugueWorkflow()
    w.df([[0], [1]], "a:int", data_determiner=to_uuid).partition(
        by=["a"], presort="b DESC", num="ROWCOUNT/2"
    ).out_transform(mock_transformer, params=dict(n=2))
    assert_eq(
        """
    create [[0],[1]] schema a:int
    
    outtransform 
        prepartition ROWCOUNT / 2 by a presort b desc
        using mock_transformer(n=2)
    """,
        w,
    )


def test_output():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    a.partition(num=4).output(mock_output)
    b = dag.create(mock_create1, params=dict(n=2))
    dag.output(a, b, using=mock_output, params=dict(n=3))
    assert_eq(
        """
    a=create using mock_create1(n=1)
    output prepartition 4 using mock_output
    output a, (create using mock_create1(n=2)) using mock_output(n=3)
    """,
        dag,
    )


def test_persist_checkpoint_broadcast():
    dag = FugueWorkflow()
    dag.create(mock_create1).persist()
    dag.create(mock_create1).weak_checkpoint(lazy=True, level="a.b")

    dag.create(mock_create1).broadcast()
    dag.create(mock_create1).weak_checkpoint(level="a.b").broadcast()

    dag.create(mock_create1).checkpoint()
    dag.create(mock_create1).strong_checkpoint(lazy=True)
    dag.create(mock_create1).strong_checkpoint(lazy=True, x="xy z")
    dag.create(mock_create1).strong_checkpoint(
        lazy=False, partition=PartitionSpec(num=5), single=True, x="xy z"
    ).broadcast()

    dag.create(mock_create1).deterministic_checkpoint()
    dag.create(mock_create1).deterministic_checkpoint(
        lazy=False, partition=PartitionSpec(num=4), single=True, namespace="n", x=2
    )
    assert_eq(
        """
    create using mock_create1 persist
    a=create using mock_create1 lazy persist (level="a.b")

    create using mock_create1 broadcast
    a=create using mock_create1 persist(level="a.b") broadcast

    create using mock_create1 checkpoint
    a= create using mock_create1 lazy strong checkpoint
    a=create using mock_create1 lazy checkpoint(x="xy z")
    a=create using mock_create1 checkpoint prepartition 5 single (x="xy z") broadcast

    create using mock_create1 deterministic checkpoint
    create using mock_create1 deterministic checkpoint "n"
        prepartition 4 single params x=2
    """,
        dag,
    )


def test_yield():
    dag = FugueWorkflow()
    dag.create(mock_create1).yield_as("a")
    dag.create(mock_create1).yield_as("aa")
    dag.create(mock_create1).deterministic_checkpoint().yield_as("c")
    dag.create(mock_create1).deterministic_checkpoint().yield_as("bb")
    dag.create(mock_create1).deterministic_checkpoint().yield_as("cc")

    assert_eq(
        """
    a=create using mock_create1 yield
    create using mock_create1 yield as aa
    c=create using mock_create1 deterministic checkpoint yield
    d=create using mock_create1 deterministic checkpoint yield as bb
    create using mock_create1 deterministic checkpoint yield as cc
    """,
        dag,
    )


def test_select_nested():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    b = dag.create(mock_create1, params=dict(n=2))
    dag.select("select * from (select * from a.b)")
    dag.select("select * from", dag.create(mock_create1), "AS bb")
    dag.select("select * from", dag.create(mock_create1), "TABLESAMPLE (5 PERCENT)")
    dag.select("select * from (select * from", dag.create(mock_create1), ")")
    assert_eq(
        """
    a=create using mock_create1(n=1)
    b=create using mock_create1(n=2)
    
    # nested query
    select * from (select * from a.b)
    select * from (create using mock_create1) AS bb
    select * from (create using mock_create1) TABLESAMPLE(5 PERCENT)
    select * from (select * from (create using mock_create1))
    """,
        dag,
    )


def test_select():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    b = dag.create(mock_create1, params=dict(n=2))
    dag.select("select * from a.b")
    dag.select("select * from a.b TABLESAMPLE (5 PERCENT) AS x")
    dag.select("select * from a.b AS x")
    dag.select("select * from", a, "AS a")  # fugue sql adds 'AS a'
    dag.select("select * from", a, "TABLESAMPLE (5 PERCENT) AS a")
    x = dag.select("select * from", a, "TABLESAMPLE (5 PERCENT) AS x")
    y = dag.select("select * FROM", x)
    z = dag.select("select * FROM", y, "where t = 100")
    dag.select("select a.* from", a, "AS a join", b, "AS b on a.a == b.a")

    dag.select("select * from", a, "AS a").persist().broadcast().show()
    dag.select("select * from", a, "AS a").weak_checkpoint(
        level="a.b.c"
    ).broadcast().show()
    assert_eq(
        """
    a=create using mock_create1(n=1)
    b=create using mock_create1(n=2)
    
    # assignment and table not found
    x=select * from a.b
    
    # sample and alias when table not found
    select * from a.b TABLESAMPLE (5 PERCENT) AS x
    select * from a.b AS x
    
    # when table is found
    select * from a
    select * from a TABLESAMPLE(5 PERCENT)
    select * from a TABLESAMPLE(5 PERCENT) AS x

    # no from
    select *
    select * where t=100

    # multiple dependencies
    select a.* from a join b on a.a==b.a

    # persist & checkpoint & broadcast
    select * from a persist broadcast print
    select * from a persist (level="a.b.c") broadcast print
    """,
        dag,
    )


def test_select_with():
    dag = FugueWorkflow()
    dag.select(
        "with x as ( select * from a ) , y as ( select * from b ) "
        "select * from x union select * from y"
    )
    assert_eq(
        """
    with
        x as (select * from a),
        y as (select * from b)
    select *   from x union select * from y
    
    """,
        dag,
    )


def test_select_plus_engine():
    class MockEngine(SqliteEngine):
        def __init__(self, execution_engine, p: int = 0):
            super().__init__(execution_engine)
            self.p = p

    dag = FugueWorkflow()
    dag.select("select * from xyz", sql_engine=MockEngine)
    dag.select("select * from xyz", sql_engine=MockEngine, sql_engine_params={"p": 2})

    temp = dag.select("select a , b from a", sql_engine=MockEngine)
    temp.transform(mock_transformer2)

    temp = dag.select("select aa , bb from a", sql_engine=MockEngine)
    dag.select("select aa + bb as t from", temp)
    assert_eq(
        """
    connect MockEngine select * from xyz
    connect MockEngine(p=2) select * from xyz
    
    transform (connect MockEngine select a,b from a) using mock_transformer2

    select aa+bb as t from (connect MockEngine select aa,bb from a)
    """,
        dag,
    )


def test_general_set_op():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    b = dag.create(mock_create1, params=dict(n=2))
    dag.select("select * from", a, "AS a union all select * from", b, "AS b")
    dag.select(
        "SELECT * FROM", dag.create(mock_create1), "union select * from", b, "AS b"
    )
    dag.select(
        "SELECT * FROM",
        dag.create(mock_create1),
        "intersect distinct SELECT * FROM",
        a.process(mock_processor1),
    )
    dag.select(
        "select * from",
        dag.create(mock_create1),
        "union SELECT * FROM",
        a.process(mock_processor1),
    )
    c = dag.create(mock_create1, params=dict(n=2))
    dag.select(
        "SELECT * FROM",
        c.transform(mock_transformer2),
        "union SELECT * FROM",
        c.process(mock_processor1),
    )
    assert_eq(
        """
    a=create using mock_create1(n=1)
    b=create using mock_create1(n=2)
    
    select * from a union all select * from b
    create using mock_create1 union select * from b
    create using mock_create1 intersect distinct process a using mock_processor1
    select * from (create using mock_create1) union process a using mock_processor1

    # operation on omitted dependencies should work as expected
    c=create using mock_create1(n=2)
    transform using mock_transformer2 union process using mock_processor1
    """,
        dag,
    )


def test_print():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    a.show()
    b = dag.create(mock_create1, params=dict(n=2))
    dag.show(a, b, rows=5, show_count=True, title='"b   B')
    assert_eq(
        """
    a=create using mock_create1(n=1)
    print
    print a, (create using mock_create1(n=2)) rows 5 rowcount title "\\"b   B"
    """,
        dag,
    )


def test_save():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    a.save("xx", fmt="parquet", mode="overwrite")
    a.save("xx", mode="append")
    a.save("xx", mode="error")
    a.save("xx.csv", fmt="csv", mode="error", single=True, header=True)
    a.partition(by=["x"]).save("xx", mode="overwrite")
    b = dag.create(mock_create1, params=dict(n=2)).save("xx", mode="overwrite")
    assert_eq(
        """
    a=create using mock_create1(n=1)
    save overwrite parquet "xx"
    save a append "xx"
    save a to "xx"
    save to single csv "xx.csv"(header=True)
    save prepartition by x overwrite "xx"
    save (create using mock_create1(n=2)) overwrite "xx"
    """,
        dag,
    )


def test_save_and_use():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    b = dag.create(mock_create1, params=dict(n=1))
    a = a.save_and_use("xx", fmt="parquet", mode="overwrite")
    b.save_and_use("xx", mode="append")
    b.save_and_use("xx", mode="error")
    a = a.save_and_use("xx.csv", fmt="csv", mode="error", single=True, header=True)
    a = a.partition(by=["x"]).save_and_use("xx", mode="overwrite")
    dag.create(mock_create1, params=dict(n=2)).save_and_use("xx", mode="overwrite")
    assert_eq(
        """
    a=create using mock_create1(n=1)
    b=create using mock_create1(n=1)
    a=save and use a overwrite parquet "xx"
    save and use b append "xx"
    save and use b to "xx"
    save and use a to single csv "xx.csv"(header=True)
    save and use prepartition by x overwrite "xx"
    save and use (create using mock_create1(n=2)) overwrite "xx"
    """,
        dag,
    )


def test_load():
    dag = FugueWorkflow()
    dag.load("xx")
    dag.load("xx", fmt="csv")
    dag.load("xx", columns="a:int,b:str")
    dag.load("xx", columns=["a", "b"], header=True)
    assert_eq(
        """
    load "xx"
    load csv "xx"
    load "xx" columns a:int, b:str
    load "xx"(header=True) columns a, b
    """,
        dag,
    )


def test_rename():
    dag = FugueWorkflow()
    a = dag.create(mock_create1)
    b = a.rename({"a": "aa", "b": "bb"})
    c = a.rename({"a": "aaa", "b": "bbb"})

    assert_eq(
        """
    a=create using mock_create1
    rename columns a:aa,b:bb
    rename columns a:aaa,b:bbb from a
    """,
        dag,
    )


def test_alter_columns():
    dag = FugueWorkflow()
    a = dag.create(mock_create1)
    a.alter_columns(Schema("a:str,b:str"))
    a.alter_columns(Schema("a:float,b:double"))

    assert_eq(
        """
    a=create using mock_create1
    alter columns a:str, b:str
    alter columns a:float, b:double from a
    """,
        dag,
    )


def test_drop():
    dag = FugueWorkflow()
    a = dag.create(mock_create1)
    b = a.drop(["a", "b"])
    c = a.drop(["a", "b"], if_exists=True)

    d = dag.create(mock_create1)
    e = d.dropna(how="any")
    f = d.dropna(how="all")
    g = d.dropna(how="any", subset=["a", "c"])
    assert_eq(
        """
    a=create using mock_create1
    drop columns a,b
    drop columns a,b if exists from a
    
    d=create using mock_create1
    drop rows if any null
    drop rows if all null from d
    drop rows if any nulls on a,c from d
    """,
        dag,
    )


def test_sample():
    dag = FugueWorkflow()
    a = dag.create(mock_create1)
    a.sample(frac=0.1, replace=False, seed=None)
    a.sample(n=5, replace=True, seed=7)

    assert_eq(
        """
    a=create using mock_create1
    sample 10 percent
    sample replace 5 rows seed 7 from a
    """,
        dag,
    )


def test_fill():
    dag = FugueWorkflow()
    a = dag.df([[None, 1], [1, None]], "a:int, b:int", data_determiner=to_uuid)
    b = a.fillna({"a": 99, "b": -99})
    assert_eq(
        """
    a=create [[NULL, 1],[1, NULL]] schema a:int, b:int
    fill nulls params a:99, b:-99 from a""",
        dag,
    )
    assert_eq(
        """
    create [[NULL, 1],[1, NULL]] schema a:int, b:int
    fill nulls (a:99, b:-99)""",
        dag,
    )


def test_head():
    dag = FugueWorkflow()
    a = dag.df(
        [[None, 1], [None, 2], [1, None], [1, 2]],
        "a:double, b:double",
        data_determiner=to_uuid,
    )
    b = a.partition(by=["a"], presort="b desc").take(1, na_position="first")
    c = b.take(1, presort="b desc", na_position="first")
    assert_eq(
        """
    a=create [[NULL, 1], [NULL, 2], [1, NULL], [1, 2]] schema a:double, b:double
    b=take 1 row from a prepartition by a presort b desc nulls first
    c=take 1 row from b presort b desc nulls first""",
        dag,
    )
    # anonymous
    assert_eq(
        """
    create [[NULL, 1], [NULL, 2], [1, NULL], [1, 2]] schema a:double, b:double
    take 1 row prepartition by a presort b desc nulls first
    take 1 row presort b desc nulls first""",
        dag,
    )


def test_module():
    # pylint: disable=no-value-for-parameter

    def create(wf: FugueWorkflow, n: int = 1) -> WorkflowDataFrame:
        return wf.df([[n]], "a:int")

    def merge(
        df1: WorkflowDataFrame, df2: WorkflowDataFrame, k: str = "aa"
    ) -> WorkflowDataFrames:
        return WorkflowDataFrames({k: df1, "bb": df2})

    def merge2(
        wf: FugueWorkflow, dfs: WorkflowDataFrames, k: int = 0
    ) -> WorkflowDataFrame:
        return dfs[k]

    def merge3(df1: WorkflowDataFrame, df2: WorkflowDataFrame) -> WorkflowDataFrames:
        return WorkflowDataFrames(df1, df2)

    @module()
    def out1(wf: FugueWorkflow, df: WorkflowDataFrame) -> None:
        df.show()

    dag = FugueWorkflow()
    a = create(dag)
    b = create(dag, n=2)
    dfs = merge(a, b, k="a1")
    dfs["a1"].show()
    dfs["bb"].show()
    df = merge2(dag, WorkflowDataFrames(a, b), k=1)
    out1(df)
    dfs = merge3(b, a)
    dfs[0].show()
    dfs[1].show()

    assert_eq(
        """
    a=sub using create
    b=sub using create(n=2)
    dfs=sub a,b using merge(k="a1")
    print dfs[a1]
    print dfs[bb]
    sub a,b using merge2(k=1)
    sub using out1
    dfs=sub df2:a,df1:b using merge3
    print dfs[0]
    print dfs[1]
    """,
        dag,
    )


def assert_eq(expr, expected: FugueWorkflow):
    global_vars, local_vars = get_caller_global_local_vars()
    sql = FugueSQL(expr, "fugueLanguage", ignore_case=True, simple_assign=True)
    wf = FugueWorkflow()
    v = _Extensions(
        sql, FugueSQLHooks(), wf, global_vars=global_vars, local_vars=local_vars
    )
    obj = v.visit(sql.tree)
    assert expected.spec_uuid() == v.workflow.spec_uuid()


# schema: a:int,b:int
def mock_create1(n=2) -> List[List[Any]]:
    return [[n, n]]


def mock_create2(n=2) -> List[List[Any]]:
    return [[n]]


# schema: b:int
def mock_processor1(
    df1: List[List[Any]], df2: List[List[Any]], n=1
) -> Iterable[List[Any]]:
    for i in range(len(df1)):
        row1 = df1[i]
        row2 = df2[i]
        yield [max(row1[0], row2[0]) + n]


def mock_processor2(
    df1: List[List[Any]], df2: List[List[Any]], n=1
) -> Iterable[List[Any]]:
    for i in range(len(df1)):
        row1 = df1[i]
        row2 = df2[i]
        yield [max(row1[0], row2[0]) + n]


# schema: a:int
def mock_processor3(df: List[List[Any]]) -> List[List[Any]]:
    return df


def mock_transformer(df: LocalDataFrame, n=0) -> LocalDataFrame:
    pass


# schema: *
def mock_transformer2(df: LocalDataFrame, n=0) -> LocalDataFrame:
    pass


# schema: b:int
def mock_cotransformer1(
    df1: List[List[Any]], df2: List[List[Any]], n=1
) -> Iterable[List[Any]]:
    for i in range(len(df1)):
        row1 = df1[i]
        row2 = df2[i]
        yield [max(row1[0], row2[0]) + n]


def mock_cotransformer2(
    df1: List[List[Any]], df2: List[List[Any]], n=1
) -> Iterable[List[Any]]:
    for i in range(len(df1)):
        row1 = df1[i]
        row2 = df2[i]
        yield [max(row1[0], row2[0]) + n]


def mock_output(dfs: DataFrames, n=1) -> None:
    pass
