from typing import Iterable, List, Tuple

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener
from antlr4.tree.Tree import TerminalNode, Token, Tree
from fugue_sql._antlr import FugueSQLLexer, FugueSQLParser
from fugue_sql.exceptions import FugueSQLSyntaxError


class FugueSQL(object):
    def __init__(
        self,
        code: str,
        rule: str,
        ignore_case: bool = False,
        simple_assign: bool = True,
        ansi_sql: bool = False,
    ):
        self._rule = rule
        self._raw_code = code
        if ignore_case:
            self._code, self._tree = _to_cased_code(
                code, rule, simple_assign=simple_assign, ansi_sql=ansi_sql
            )
        else:
            self._code = code
            self._tree = _to_tree(
                self._code,
                self._rule,
                False,
                simple_assign=simple_assign,
                ansi_sql=ansi_sql,
            )

    @property
    def raw_code(self):  # pragma: no cover
        return self._raw_code

    @property
    def code(self):  # pragma: no cover
        return self._code

    @property
    def tree(self) -> Tree:  # pragma: no cover
        return self._tree


def _to_tree(
    code: str, rule: str, all_upper_case: bool, simple_assign: bool, ansi_sql: bool
) -> Tree:
    input_stream = InputStream(code)
    lexer = FugueSQLLexer(input_stream)
    lexer._all_upper_case = all_upper_case
    lexer._ansi_sql = ansi_sql
    lexer._simple_assign = simple_assign
    stream = CommonTokenStream(lexer)
    parser = FugueSQLParser(stream)
    parser._all_upper_case = all_upper_case
    parser._simple_assign = simple_assign
    parser._ansi_sql = ansi_sql
    parser.addErrorListener(_ErrorListener(code.splitlines()))
    return getattr(parser, rule)()  # validate syntax


def _to_cased_code(
    code: str, rule: str, simple_assign: bool, ansi_sql: bool
) -> Tuple[str, Tree]:
    tree = _to_tree(
        code.upper(), rule, True, simple_assign=simple_assign, ansi_sql=ansi_sql
    )
    tokens = [t for t in _to_tokens(tree) if _is_keyword(t)]
    start = 0
    cased_code: List[str] = []
    for t in tokens:
        if t.start > start:
            cased_code.append(code[start : t.start])
        cased_code.append(code[t.start : t.stop + 1].upper())
        start = t.stop + 1
    if start < len(code):
        cased_code.append(code[start:])
    return "".join(cased_code), tree


def _to_tokens(node: Tree) -> Iterable[Token]:
    if isinstance(node, TerminalNode):
        yield node.getSymbol()
    else:
        for i in range(node.getChildCount()):
            for x in _to_tokens(node.getChild(i)):
                yield x


def _is_keyword(token: Token):
    if not hasattr(FugueSQLParser, token.text):
        return False
    return getattr(FugueSQLParser, token.text) == token.type


class _ErrorListener(ErrorListener):
    def __init__(self, lines: List[str]):
        super().__init__()
        self._lines = lines

    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        raise FugueSQLSyntaxError(
            f"{msg}\nline {line}: {self._lines[line - 1]}\n{offendingSymbol}"
        )

    def reportAmbiguity(
        self, recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs
    ):  # pragma: no cover
        pass

    def reportAttemptingFullContext(
        self, recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs
    ):  # pragma: no cover
        pass

    def reportContextSensitivity(
        self, recognizer, dfa, startIndex, stopIndex, prediction, configs
    ):  # pragma: no cover
        pass
