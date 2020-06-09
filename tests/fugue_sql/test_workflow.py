import json
from typing import Any, Iterable, List

from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrame, DataFrames, LocalDataFrame
from fugue.workflow.workflow import FugueWorkflow
from fugue_sql.exceptions import FugueSQLError
from fugue_sql.parse import FugueSQL
from fugue_sql.visitors import _Extensions, _VisitorBase
from triad.collections.schema import Schema
from pytest import raises


def test_create_data():
    w = FugueWorkflow().df([[0], [1]], "a:int")
    assert_eq("""
    a=create [[0],[1]] schema a:int
    """, w.workflow)


def test_create():
    dag = FugueWorkflow()
    dag.create(mock_create1, params=dict(n=1))
    dag.create(mock_create2, schema="a:int", params=dict(n=1))
    assert_eq("""
    a=create using mock_create1 params n:1
    b=create using mock_create2(n=1) schema a:int
    """, dag)


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
        using=mock_processor1, params=dict(n=7))
    assert_eq("""
    a=create using mock_create1 params n:1
    b=create using mock_create1 params n:2
    process a,b using mock_processor1(n=3)
    process b,a using mock_processor2(n=4) schema b:int
    process  # nested
        (create using mock_create1(n=5)),
        (create using mock_create1(n=6))
        using mock_processor1(n=7)
    """, dag)

    # anonymous, nested anonymous
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1)).process(mock_processor3)
    b = a.partition(by=["a"]).process(mock_processor3)
    c = a.process(mock_processor3)
    dag.process(b, c, using=mock_processor1)
    assert_eq("""
    create using mock_create1 params n:1
    process using mock_processor3
    process  # nested
        (process prepartition by a using mock_processor3),
        (process using mock_processor3)
        using mock_processor1
    """, dag)

    # no last dataframe
    with raises(FugueSQLError):
        assert_eq("""
        process using mock_processor3
        """, None)

    # dict like dataframes
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    b = dag.create(mock_create1, params=dict(n=2))
    dag.process(dict(df1=a, df2=b), using=mock_processor1)
    assert_eq("""
    process
        df1=(create using mock_create1(n=1)),
        df2:(create using mock_create1(n=2))
        using mock_processor1
    """, dag)


def test_zip():
    dag = FugueWorkflow()
    a1 = dag.create(mock_create1, params=dict(n=1))
    a2 = dag.create(mock_create1, params=dict(n=2))
    a1.zip(a2)
    assert_eq("""
    a=create using mock_create1 params n:1
    zip a,(create using mock_create1 params n:2)
    """, dag)

    dag = FugueWorkflow()
    a1 = dag.create(mock_create1, params=dict(n=1))
    a2 = dag.create(mock_create1, params=dict(n=2))
    a1.zip(a2, how="left_outer", partition=dict(by=["a"], presort="b DESC"))
    assert_eq("""
    a=create using mock_create1 params n:1
    zip a,(create using mock_create1 params n:2) left
        outer by a presort b desc
    """, dag)


def test_cotransform():
    dag = FugueWorkflow()
    a1 = dag.create(mock_create1, params=dict(n=1))
    a2 = dag.create(mock_create1, params=dict(n=2))
    z = dag.zip(a1, a2)
    t = z.partition(num=3).transform(mock_cotransformer1, params=dict(n=3))
    assert_eq("""
    zip 
        (create using mock_create1 params n:1),
        (create using mock_create1 params n:2)
    transform prepartition 3 using mock_cotransformer1(n=3)
    """, dag)


def test_transform():
    w = (FugueWorkflow().df([[0], [1]], "a:int")
         .transform(mock_transformer, schema=Schema("a:int"), params=dict(n=2))
         )
    assert_eq("""
    create [[0],[1]] schema a:int
    transform using mock_transformer(n=2) schema a:int
    """, w.workflow)

    w = (FugueWorkflow().df([[0], [1]], "a:int")
         .partition(by=["a"], presort="b DESC", num="ROWCOUNT/2")
         .transform(mock_transformer, schema="*", params=dict(n=2))
         )
    assert_eq("""
    create [[0],[1]] schema a:int
    
    transform 
        prepartition ROWCOUNT / 2 by a presort b desc
        using mock_transformer(n=2) schema *
    """, w.workflow)


def test_output():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    a.partition(num=4).output(mock_output)
    b = dag.create(mock_create1, params=dict(n=2))
    dag.output(a, b, using=mock_output, params=dict(n=3))
    assert_eq("""
    a=create using mock_create1(n=1)
    output prepartition 4 using mock_output
    output a, (create using mock_create1(n=2)) using mock_output(n=3)
    """, dag)


def test_select():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    b = dag.create(mock_create1, params=dict(n=2))
    dag.select("select * from a.b")
    dag.select("select * from a.b TABLESAMPLE (5 PERCENT) AS x")
    dag.select("select * from a.b AS x")
    dag.select("select * from", a, "AS a")  # fugue sql adds 'AS a'
    dag.select("select * from", a, "TABLESAMPLE (5 PERCENT) AS a")
    dag.select("select * from", a, "TABLESAMPLE (5 PERCENT) AS x")
    dag.select("select a.* from", a, "AS a join", b, "AS b on a.a == b.a")

    dag.select("select * from (select * from a.b)")
    dag.select("select * from", dag.create(mock_create1), "TABLESAMPLE (5 PERCENT)")
    dag.select("select * from", dag.create(mock_create1), "AS b")
    dag.select("select * from (select * from", dag.create(mock_create1), ")")
    assert_eq("""
    a=create using mock_create1(n=1)
    b=create using mock_create1(n=2)
    
    # assignment and table not found
    x:=select * from a.b
    
    # sample and alias when table not found
    select * from a.b TABLESAMPLE (5 PERCENT) AS x
    select * from a.b AS x
    
    # when table is found
    select * from a
    select * from a TABLESAMPLE(5 PERCENT)
    select * from a TABLESAMPLE(5 PERCENT) AS x

    # multiple dependencies
    select a.* from a join b on a.a==b.a

    # nested query
    select * from (select * from a.b)

    # nested fugue extensions
    select * from (create using mock_create1) TABLESAMPLE(5 PERCENT)
    select * from (create using mock_create1) AS b
    select * from (select * from (create using mock_create1))
    """, dag)


def test_print():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    a.show()
    b = dag.create(mock_create1, params=dict(n=2))
    dag.show(a, b, rows=5, show_count=True, title="\"b   B")
    assert_eq("""
    a=create using mock_create1(n=1)
    print
    print a, (create using mock_create1(n=2)) rows 5 rowcount title "\\"b   B"
    """, dag)


def test_save():
    dag = FugueWorkflow()
    a = dag.create(mock_create1, params=dict(n=1))
    a.save("xx", fmt="parquet", mode="overwrite")
    a.save("xx", mode="append")
    a.save("xx", mode="error")
    a.save("xx.csv", fmt="csv", mode="error", single=True, header=True)
    a.partition(by=["x"]).save("xx", mode="overwrite")
    b = dag.create(mock_create1, params=dict(n=2)).save("xx", mode="overwrite")
    assert_eq("""
    a=create using mock_create1(n=1)
    save overwrite parquet "xx"
    save a append "xx"
    save a to "xx"
    save to single csv "xx.csv"(header=True)
    save prepartition by x overwrite "xx"
    save (create using mock_create1(n=2)) overwrite "xx"
    """, dag)


def assert_eq(expr, expected: FugueWorkflow):
    sql = FugueSQL(expr, "fugueLanguage", ignore_case=True, simple_assign=True)
    wf = FugueWorkflow()
    v = _Extensions(sql, wf)
    obj = v.visit(sql.tree)
    assert expected.spec_uuid() == v.workflow.spec_uuid()


# schema: a:int
def mock_create1(n=2) -> List[List[Any]]:
    return [[n]]


def mock_create2(n=2) -> List[List[Any]]:
    return [[n]]


# schema: b:int
def mock_processor1(
    df1: List[List[Any]],
    df2: List[List[Any]],
    n=1
) -> Iterable[List[Any]]:
    for i in range(len(df1)):
        row1 = df1[i]
        row2 = df2[i]
        yield [max(row1[0], row2[0]) + n]


def mock_processor2(
    df1: List[List[Any]],
    df2: List[List[Any]],
    n=1
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


# schema: b:int
def mock_cotransformer1(
    df1: List[List[Any]],
    df2: List[List[Any]],
    n=1
) -> Iterable[List[Any]]:
    for i in range(len(df1)):
        row1 = df1[i]
        row2 = df2[i]
        yield [max(row1[0], row2[0]) + n]


def mock_cotransformer2(
    df1: List[List[Any]],
    df2: List[List[Any]],
    n=1
) -> Iterable[List[Any]]:
    for i in range(len(df1)):
        row1 = df1[i]
        row2 = df2[i]
        yield [max(row1[0], row2[0]) + n]


def mock_output(dfs: DataFrames, n=1) -> None:
    pass
