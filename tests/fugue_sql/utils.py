import itertools

from antlr4.tree.Tree import TerminalNode
from fugue_sql.exceptions import FugueSQLSyntaxError
from fugue_sql._parse import FugueSQL
from pytest import raises


def good_single_syntax(*code, ignore_case=True, simple_assign=False,
                       ansi_sql=False, show=False):
    good_syntax(*code, rule="fugueSingleStatement",
                ignore_case=ignore_case, simple_assign=simple_assign, ansi_sql=ansi_sql, show=show)


def good_syntax(*code, rule="fugueLanguage", ignore_case=True,
                simple_assign=False, ansi_sql=False, show=False):
    for c in _enum_comb(*code):
        s = FugueSQL(c, rule, ignore_case=ignore_case,
                     simple_assign=simple_assign, ansi_sql=ansi_sql)
        if show:
            _print_tree(s.tree)


def bad_single_syntax(*code, ignore_case=True, simple_assign=False, ansi_sql=False):
    bad_syntax(*code, rule="fugueSingleStatement",
               ignore_case=ignore_case, simple_assign=simple_assign, ansi_sql=ansi_sql)


def bad_syntax(*code, rule="fugueLanguage", ignore_case=True,
               simple_assign=False, ansi_sql=False):
    for c in _enum_comb(*code):
        with raises(FugueSQLSyntaxError):
            FugueSQL(c, rule, ignore_case=ignore_case,
                     simple_assign=simple_assign, ansi_sql=ansi_sql)


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


def _print_tree(node, prefix=""):
    if isinstance(node, TerminalNode):
        token = node.getSymbol()
        print(prefix, type(token).__name__, token.text)
    else:
        print(prefix, type(node).__name__)
        for i in range(node.getChildCount()):
            _print_tree(node.getChild(i), prefix + "  ")
