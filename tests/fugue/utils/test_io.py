import os
import sys

import pytest
from pytest import raises
from triad.utils.io import makedirs, read_text, touch

from fugue._utils.io import _FORMAT_MAP, FileParser, load_df, save_df
from fugue.dataframe.array_dataframe import ArrayDataFrame
from fugue.dataframe.pandas_dataframe import PandasDataFrame
from fugue.dataframe.utils import _df_eq as df_eq


@pytest.mark.skipif(sys.platform.startswith("win"), reason="not a test for windows")
def test_file_parser_linux():
    f = FileParser("/a/b/c.parquet")
    assert "/a/b/c.parquet" == f.uri
    assert "/a/b/c.parquet" == f.uri_with_glob
    assert ".parquet" == f.suffix
    assert "parquet" == f.file_format
    assert "" == f.glob_pattern
    assert "/a/b" == f.parent


@pytest.mark.skipif(
    not sys.platform.startswith("win"), reason="a test only for windows"
)
def test_file_parser_win():
    f = FileParser("c:\\a\\c.parquet")
    assert "c:\\a\\c.parquet" == f.uri
    assert "c:\\a\\c.parquet" == f.uri_with_glob
    assert ".parquet" == f.suffix
    assert "parquet" == f.file_format
    assert "" == f.glob_pattern
    assert "c:\\a" == f.parent

    f = FileParser("c:\\a\\*.parquet")
    assert "c:\\a" == f.uri
    assert "c:\\a\\*.parquet" == f.uri_with_glob
    assert ".parquet" == f.suffix
    assert "parquet" == f.file_format
    assert "*.parquet" == f.glob_pattern
    assert "c:\\" == f.parent


def test_file_parser():
    f = FileParser("c.parquet")
    assert "c.parquet" == f.uri
    assert "c.parquet" == f.uri_with_glob
    assert ".parquet" == f.suffix
    assert "parquet" == f.file_format
    assert "" == f.glob_pattern
    assert "." == f.parent

    for k, v in _FORMAT_MAP.items():
        f = FileParser(f"s3://a/b/c{k}")
        assert f"s3://a/b/c{k}" == f.uri
        assert k == f.suffix
        assert v == f.file_format
        assert "s3://a/b" == f.parent

    f = FileParser("s3://a/b/c.test.parquet")
    assert "s3://a/b/c.test.parquet" == f.uri
    assert ".test.parquet" == f.suffix
    assert "parquet" == f.file_format
    assert "s3://a/b" == f.parent

    f = FileParser("s3://a/b/c.ppp.gz", "csv")
    assert "s3://a/b/c.ppp.gz" == f.uri
    assert ".ppp.gz" == f.suffix
    assert "csv" == f.file_format

    f = FileParser("s3://a/b/c", "csv")
    assert "s3://a/b/c" == f.uri
    assert "" == f.suffix
    assert "csv" == f.file_format

    raises(NotImplementedError, lambda: FileParser("s3://a/b/c.ppp"))
    raises(NotImplementedError, lambda: FileParser("s3://a/b/c.parquet", "csvv"))
    raises(NotImplementedError, lambda: FileParser("s3://a/b/c"))


@pytest.mark.skipif(sys.platform.startswith("win"), reason="not a test for windows")
def test_file_parser_glob_linux():
    f = FileParser("/a/b/*.parquet")
    assert "/a/b" == f.uri
    assert ".parquet" == f.suffix
    assert "parquet" == f.file_format
    assert "*.parquet" == f.glob_pattern
    assert "/a/b/*.parquet" == f.uri_with_glob

    f = FileParser("/a/b/*123.parquet")
    assert "/a/b" == f.uri
    assert ".parquet" == f.suffix
    assert "parquet" == f.file_format
    assert "*123.parquet" == f.glob_pattern
    assert "/a/b/*123.parquet" == f.uri_with_glob


def test_file_parser_glob():
    f = FileParser("s3://a/b/*.parquet")
    assert "s3://a/b" == f.uri
    assert ".parquet" == f.suffix
    assert "parquet" == f.file_format
    assert "*.parquet" == f.glob_pattern
    assert "s3://a/b/*.parquet" == f.uri_with_glob

    ff = FileParser("s3://a/b", "parquet").with_glob("*.csv", "csv")
    assert "s3://a/b/*.csv" == ff.uri_with_glob
    assert "csv" == ff.file_format
    ff = FileParser("s3://a/b/", "csv").with_glob("*.csv")
    assert "s3://a/b/*.csv" == ff.uri_with_glob
    assert "csv" == ff.file_format
    ff = FileParser("s3://a/b/*.parquet").with_glob("*.csv")
    assert "s3://a/b/*.csv" == ff.uri_with_glob
    assert "csv" == ff.file_format
    ff = FileParser("s3://a/b/*.parquet", "parquet").with_glob("*.csv")
    assert "s3://a/b/*.csv" == ff.uri_with_glob
    assert "parquet" == ff.file_format
    ff = FileParser("s3://a/b/*.parquet", "parquet").with_glob("*.csv", "csv")
    assert "s3://a/b/*.csv" == ff.uri_with_glob
    assert "csv" == ff.file_format


def test_parquet_io(tmpdir):
    df1 = PandasDataFrame([["1", 2, 3]], "a:str,b:int,c:long")
    df2 = ArrayDataFrame([[[1, 2]]], "a:[int]")
    # {a:int} will become {a:long} because pyarrow lib has issue
    df3 = ArrayDataFrame([[dict(a=1)]], "a:{a:long}")
    for df in [df1, df2, df3]:
        path = os.path.join(tmpdir, "a.parquet")
        save_df(df, path)
        actual = load_df(path)
        df_eq(df, actual, throw=True, check_order=True)

    save_df(df1, path)
    actual = load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")
    actual = load_df(path, columns="b:str,a:int")
    df_eq(actual, [["2", 1]], "b:str,a:int")
    # can't specify wrong columns
    raises(Exception, lambda: load_df(path, columns="bb:str,a:int"))

    # load directory
    for name in ["folder.parquet", "folder"]:
        folder = os.path.join(tmpdir, name)
        makedirs(folder)
        f0 = os.path.join(folder, "_SUCCESS")
        f1 = os.path.join(folder, "1.parquet")
        f2 = os.path.join(folder, "3.parquet")
        touch(f0)
        save_df(df1, f1)
        save_df(df1, f2)

    actual = load_df(folder, "parquet")
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # load multiple paths
    actual = load_df([f1, f2], "parquet")
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # load folder
    actual = load_df(folder, "parquet")
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    actual = load_df(os.path.join(tmpdir, "folder.parquet"))
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # load pattern
    actual = load_df(os.path.join(tmpdir, "folder", "*.parquet"))
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # overwrite folder with single file
    save_df(actual, os.path.join(tmpdir, "folder.parquet"), mode="overwrite")
    actual = load_df(os.path.join(tmpdir, "folder.parquet"))
    df_eq(actual, [["1", 2, 3], ["1", 2, 3]], "a:str,b:int,c:long")

    # overwrite = False
    raises(FileExistsError, lambda: save_df(df1, f1, mode="error"))
    raises(
        FileExistsError,
        lambda: save_df(df1, os.path.join(tmpdir, "folder.parquet"), mode="error"),
    )

    # wrong mode
    raises(NotImplementedError, lambda: save_df(df1, f1, mode="dummy"))


def test_csv_io(tmpdir):
    df1 = PandasDataFrame([["1", 2, 3]], "a:str,b:int,c:long")
    path = os.path.join(tmpdir, "a.csv")
    # without header
    save_df(df1, path)
    assert read_text(path).startswith("1,2,3")
    raises(ValueError, lambda: load_df(path, header=False))
    actual = load_df(path, columns=["a", "b", "c"], header=False, infer_schema=True)
    assert [[1, 2, 3]] == actual.as_array()
    assert actual.schema == "a:long,b:long,c:long"
    actual = load_df(path, columns="a:double,b:str,c:str", header=False)
    assert [[1.0, "2", "3"]] == actual.as_array()
    assert actual.schema == "a:double,b:str,c:str"
    # with header
    save_df(df1, path, header=True)
    assert read_text(path).startswith("a,b,c")
    actual = load_df(path, header=True)
    assert [["1", "2", "3"]] == actual.as_array()
    actual = load_df(path, header=True, infer_schema=True)
    assert [[1, 2, 3]] == actual.as_array()
    actual = load_df(path, columns=["b", "a"], header=True, infer_schema=True)
    assert [[2, 1]] == actual.as_array()
    actual = load_df(path, columns="b:str,a:double", header=True)
    assert [["2", 1.0]] == actual.as_array()
    raises(KeyError, lambda: load_df(path, columns="b:str,x:double", header=True))

    raises(
        NotImplementedError, lambda: load_df(path, columns="b:str,x:double", header=2)
    )


def test_json(tmpdir):
    df1 = PandasDataFrame([["1", 2, 3]], "a:str,b:int,c:long")
    path = os.path.join(tmpdir, "a.json")
    save_df(df1, path)
    actual = load_df(path)
    df_eq(actual, [[1, 2, 3]], "a:long,b:long,c:long")
    actual = load_df(path, columns=["b", "a"])
    df_eq(actual, [[2, "1"]], "b:int,a:str")
    actual = load_df(path, columns="b:str,a:int")
    df_eq(actual, [["2", 1]], "b:str,a:int")
    raises(KeyError, lambda: load_df(path, columns="bb:str,a:int"))
