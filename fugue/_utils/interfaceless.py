import inspect
from typing import Callable, Optional

from triad.utils.assertion import assert_or_throw

_COMMENT_SCHEMA_ANNOTATION = "schema"


def parse_comment_annotation(func: Callable, annotation: str) -> Optional[str]:
    """Parse comment annotation above the function. It try to find
    comment lines starts with the annotation from bottom up, and will use the first
    occurrance as the result.

    :param func: the function
    :param annotation: the annotation string
    :return: schema hint string

    .. admonition:: Examples

        .. code-block:: python

            # schema: a:int,b:str
            #schema:a:int,b:int # more comment
            # some comment
            def dummy():
                pass

            assert "a:int,b:int" == parse_comment_annotation(dummy, "schema:")
    """
    for orig in reversed((inspect.getcomments(func) or "").splitlines()):
        start = orig.find(":")
        if start <= 0:
            continue
        actual = orig[:start].replace("#", "", 1).strip()
        if actual != annotation:
            continue
        end = orig.find("#", start)
        s = orig[start + 1 : (end if end > 0 else len(orig))].strip()
        return s
    return None


def parse_output_schema_from_comment(func: Callable) -> Optional[str]:
    """Parse schema hint from the comments above the function. It try to find
    comment lines starts with `schema:` from bottom up, and will use the first
    occurrance as the hint.

    :param func: the function
    :return: schema hint string

    .. admonition:: Examples

        .. code-block:: python

            # schema: a:int,b:str
            #schema:a:int,b:int # more comment
            # some comment
            def dummy():
                pass

            assert "a:int,b:int" == parse_output_schema_from_comment(dummy)
    """
    res = parse_comment_annotation(func, _COMMENT_SCHEMA_ANNOTATION)
    if res is None:
        return None
    assert_or_throw(res != "", SyntaxError("incorrect schema annotation"))
    return res.strip()


def is_class_method(func: Callable) -> bool:
    sig = inspect.signature(func)
    # TODO: this is not the best way
    return "self" in sig.parameters
