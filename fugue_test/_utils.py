import pytest


def _is_spark2():
    try:
        import pyspark

        return pyspark.__version__ < "3.0.0"
    except Exception:  # pragma: no cover
        return False


skip_spark2 = pytest.mark.skipif(_is_spark2(), reason="Skip Spark<3")
