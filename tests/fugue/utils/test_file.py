from fugue.utils.file import FileParser
from pytest import raises


def test_file_parser():
    f = FileParser("/a/b/c.parquet")
    assert "/a/b/c.parquet" == f.uri
    assert "" == f.scheme
    assert "/a/b/c.parquet" == f.path
    assert ".parquet" == f.suffix
    assert "parquet" == f.file_format

    f = FileParser("s3://a/b/c.parquet")
    assert "s3://a/b/c.parquet" == f.uri
    assert "s3" == f.scheme
    assert "/b/c.parquet" == f.path
    assert ".parquet" == f.suffix
    assert "parquet" == f.file_format

    f = FileParser("s3://a/b/c.ppp.gz", "csv")
    assert "s3://a/b/c.ppp.gz" == f.uri
    assert "s3" == f.scheme
    assert "/b/c.ppp.gz" == f.path
    assert ".ppp.gz" == f.suffix
    assert "csv" == f.file_format

    f = FileParser("s3://a/b/c", "csv")
    assert "s3://a/b/c" == f.uri
    assert "s3" == f.scheme
    assert "/b/c" == f.path
    assert "" == f.suffix
    assert "csv" == f.file_format

    raises(NotImplementedError, lambda: FileParser("s3://a/b/c.ppp"))
    raises(NotImplementedError, lambda: FileParser("s3://a/b/c.parquet", "csvv"))
    raises(NotImplementedError, lambda: FileParser("s3://a/b/c"))
