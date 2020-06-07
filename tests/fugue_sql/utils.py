from fugue_sql.antlr import FugueSQLListener
from fugue_sql.exceptions import FugueSQLSyntaxError
from fugue_sql.parse import FugueSQL
from pytest import raises
import itertools


def good_single_syntax(*code, ignore_case=True, simple_assign=False):
    good_syntax(*code, rule="fugueSingleStatement",
                ignore_case=ignore_case, simple_assign=simple_assign)


def good_syntax(*code, rule="fugueLanguage", ignore_case=True, simple_assign=False):
    for c in _enum_comb(*code):
        s = FugueSQL(c, rule, ignore_case=ignore_case, simple_assign=simple_assign)
        print(s.code)


def bad_single_syntax(*code, ignore_case=True, simple_assign=False):
    bad_syntax(*code, rule="fugueSingleStatement",
               ignore_case=ignore_case, simple_assign=simple_assign)


def bad_syntax(*code, rule="fugueLanguage", ignore_case=True, simple_assign=False):
    for c in _enum_comb(*code):
        with raises(FugueSQLSyntaxError):
            FugueSQL(c, rule, ignore_case=ignore_case, simple_assign=simple_assign)


def _enum_comb(*code):
    data = []
    for c in code:
        if isinstance(c, str):
            data.append([c])
        elif isinstance(c, list):
            if len(c) == 1:
                c.append("")
            data.append(c)
        else:
            raise Exception("element can be either a string or an array: " + c)
    for l in itertools.product(*data):
        yield " ".join(l)
