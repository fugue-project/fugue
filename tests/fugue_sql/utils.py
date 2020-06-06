from fugue_sql.antlr import FugueSQLListener
from fugue_sql.exceptions import FugueSQLSyntaxError
from fugue_sql.parse import FugueSQL
from pytest import raises
import itertools


def good_single_syntax(*code, ignore_case=True):
    good_syntax(*code, rule="singleStatement", ignore_case=ignore_case)


def good_syntax(*code, rule, ignore_case=True):
    for c in _enum_comb(*code):
        s = FugueSQL(c, rule, ignore_case=ignore_case)
        print(s.code)


def bad_single_syntax(*code, ignore_case=True):
    bad_syntax(*code, rule="singleStatement", ignore_case=ignore_case)


def bad_syntax(*code, rule, ignore_case=True):
    for c in _enum_comb(*code):
        with raises(FugueSQLSyntaxError):
            FugueSQL(c, rule, ignore_case=ignore_case)


def _enum_comb(*code):
    data = []
    for c in code:
        if isinstance(c, str):
            data.append([c])
        elif isinstance(c, list):
            c.append("")
            data.append(c)
        else:
            raise Exception("element can be either a string or an array: " + c)
    for l in itertools.product(*data):
        yield " ".join(l)
