from typing import Any, Callable, Dict

from triad import Schema
from triad.utils.assertion import assert_or_throw

from fugue._utils.interfaceless import parse_comment_annotation
from fugue.collections.partition import PartitionSpec, parse_presort_exp
from fugue.exceptions import (
    FugueWorkflowCompileValidationError,
    FugueWorkflowRuntimeValidationError,
)


def is_namespace_extension(obj: Any) -> bool:
    return isinstance(obj, tuple) and len(obj) == 2 and isinstance(obj[0], str)


def load_namespace_extensions(obj: Any) -> None:
    if is_namespace_extension(obj):
        from fugue_contrib import load_namespace

        load_namespace(obj[0])


def namespace_candidate(
    namespace: str, matcher: Callable[..., bool]
) -> Callable[..., bool]:
    def _matcher(obj: Any, *args: Any, **kwargs: Any) -> bool:
        if is_namespace_extension(obj) and obj[0] == namespace:
            return matcher(obj[1], *args, **kwargs)
        return False

    return _matcher


def parse_validation_rules_from_comment(func: Callable) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    for key in [
        "partitionby_has",
        "partitionby_is",
        "presort_has",
        "presort_is",
        "input_has",
        "input_is",
    ]:
        v = parse_comment_annotation(func, key)
        if v is None:
            continue
        assert_or_throw(v != "", lambda: SyntaxError(f"{key} can't be empty"))
        res[key] = v
    return to_validation_rules(res)


def to_validation_rules(data: Dict[str, Any]) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    for k, v in data.items():
        if k in ["partitionby_has", "partitionby_is"]:
            if isinstance(v, str):
                v = [x.strip() for x in v.split(",")]
            res[k] = PartitionSpec(by=v).partition_by
        elif k in ["presort_has", "presort_is"]:
            res[k] = list(parse_presort_exp(v).items())
        elif k in ["input_has"]:
            if isinstance(v, str):
                res[k] = v.replace(" ", "").split(",")
            else:
                assert_or_throw(
                    isinstance(v, list),
                    lambda: SyntaxError(f"{v} is neither a string or a list"),
                )
                res[k] = [x.replace(" ", "") for x in v]
        elif k in ["input_is"]:
            try:
                res[k] = str(Schema(v))
            except SyntaxError:
                raise SyntaxError(  # pylint: disable=W0707
                    f"for input_is, the input must be a schema expression {v}"
                )
        else:
            raise NotImplementedError(k)
    return res


def validate_partition_spec(spec: PartitionSpec, rules: Dict[str, Any]) -> None:
    for k, v in rules.items():
        if k in ["partitionby_has", "partitionby_is"]:
            for x in v:
                assert_or_throw(
                    x in spec.partition_by,
                    lambda: FugueWorkflowCompileValidationError(
                        f"required partition key {x} is not in {spec}"
                    ),
                )
            if k == "partitionby_is":
                assert_or_throw(
                    len(v) == len(spec.partition_by),
                    lambda: FugueWorkflowCompileValidationError(
                        f"{v} does not match {spec}"
                    ),
                )
        if k in ["presort_has", "presort_is"]:
            expected = spec.presort
            for pk, pv in v:
                o = "ASC" if pv else "DESC"
                assert_or_throw(
                    pk in expected,
                    lambda: FugueWorkflowCompileValidationError(
                        f"required presort key {pk} is not in presort of {spec}"
                    ),
                )
                assert_or_throw(
                    pv == expected[pk],
                    lambda: FugueWorkflowCompileValidationError(
                        f"({pk},{o}) order does't match presort of {spec}"
                    ),
                )
            if k == "presort_is":
                assert_or_throw(
                    len(v) == len(expected),
                    lambda: FugueWorkflowCompileValidationError(
                        f"{v} does not match {spec}"
                    ),
                )
                assert_or_throw(
                    v == list(expected.items()),
                    lambda: FugueWorkflowCompileValidationError(
                        f"{v} order does not match {spec}"
                    ),
                )


def validate_input_schema(schema: Schema, rules: Dict[str, Any]) -> None:
    for k, v in rules.items():
        if k == "input_has":
            for x in v:
                assert_or_throw(
                    x in schema,
                    lambda: FugueWorkflowRuntimeValidationError(
                        f"required column {x} is not in {schema}"
                    ),
                )
        if k == "input_is":
            assert_or_throw(
                schema == v,
                lambda: FugueWorkflowRuntimeValidationError(
                    f"{v} does not match {schema}"
                ),
            )
