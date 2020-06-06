from fugue_sql.antlr import FugueSQLListener
from fugue_sql.exceptions import FugueSQLSyntaxError
from fugue_sql.parse import FugueSQL
from pytest import raises
from tests.fugue_sql.utils import good_single_syntax, bad_single_syntax


def test_syntax():
    good_single_syntax('SELECT a', ["FROM sx"], ignore_case=False)
    bad_single_syntax('select a', ["from sx"], ignore_case=False)
    good_single_syntax('select a', ["from sx"], ignore_case=True)
