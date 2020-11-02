from fugue.collections import PartitionSpec
from fugue.exceptions import (FugueWorkflowCompileValidationError,
                              FugueWorkflowRuntimeValidationError)
from fugue.extensions._utils import (parse_validation_rules_from_comment,
                                     to_validation_rules,
                                     validate_input_schema,
                                     validate_partition_spec)
from pytest import raises


def test_parse_output_schema_from_comment():
    # partitionby_has: a, b
    # partitionby_is: c , d
    # presort_has: a, b desc
    # presort_is: a asc, b desc
    # input_has: a,b: str,c
    # input_is: a: int , b : str
    # dummy: ssdsfd
    def t():
        pass

    # input_is:
    def t2():
        pass

    assert {
        "partitionby_has": ["a", "b"],
        "partitionby_is": ["c", "d"],
        "presort_has": [("a", True), ("b", False)],
        "presort_is": [("a", True), ("b", False)],
        "input_has": ["a", "b:str", "c"],
        "input_is": "a:int,b:str",
    } == parse_validation_rules_from_comment(t)

    raises(SyntaxError, lambda: parse_validation_rules_from_comment(t2))


def test_to_validation_rules():
    raises(NotImplementedError, lambda: to_validation_rules({"": ""}))
    assert {
        "partitionby_has": ["a", "b"],
        "partitionby_is": ["c", "d"],
    } == to_validation_rules({"partitionby_has": ["a", "b"], "partitionby_is": " c, d"})
    assert {
        "presort_has": [("a", True), ("b", False)],
        "presort_is": [("c", False), ("d", True)],
    } == to_validation_rules(
        {
            "presort_has": " a , b desc ",
            "presort_is": [("c", False), "d"],
        }
    )
    assert {"input_has": ["a", "b:str"],} == to_validation_rules(
        {
            "input_has": ["a", " b : str "],
        }
    )
    assert {"input_has": ["a", "b:str"],} == to_validation_rules(
        {
            "input_has": "a, b : str",
        }
    )
    assert {"input_is": "a:int,b:str"} == to_validation_rules(
        {
            "input_is": ["a:int", " b : str "],
        }
    )
    with raises(SyntaxError):
        to_validation_rules(
            {
                "input_is": "a, b : str",
            }
        )


def test_validate_partition_spec():
    spec = PartitionSpec(by=["a", "b", "c"], presort="d desc, e")

    validate_partition_spec(spec, to_validation_rules({"partitionby_has": ["a"]}))
    with raises(FugueWorkflowCompileValidationError):
        validate_partition_spec(spec, to_validation_rules({"partitionby_has": ["x"]}))
    with raises(FugueWorkflowCompileValidationError):
        validate_partition_spec(spec, to_validation_rules({"partitionby_has": "a,x"}))
    validate_partition_spec(spec, to_validation_rules({"partitionby_is": "a,b,c"}))
    # order in partitionby doesn't matter
    validate_partition_spec(spec, to_validation_rules({"partitionby_is": "c,b,a"}))
    with raises(FugueWorkflowCompileValidationError):
        validate_partition_spec(spec, to_validation_rules({"partitionby_is": "a,b"}))

    # order in presort_has doesn't matter
    validate_partition_spec(spec, to_validation_rules({"presort_has": "d desc"}))
    validate_partition_spec(spec, to_validation_rules({"presort_has": "e,d desc"}))
    with raises(FugueWorkflowCompileValidationError):  # sort order matters
        validate_partition_spec(spec, to_validation_rules({"presort_has": "d"}))
    with raises(FugueWorkflowCompileValidationError):
        validate_partition_spec(spec, to_validation_rules({"presort_has": "x"}))
    with raises(FugueWorkflowCompileValidationError):
        validate_partition_spec(spec, to_validation_rules({"presort_has": "x,d desc"}))

    validate_partition_spec(spec, to_validation_rules({"presort_is": "d desc,e"}))
    with raises(FugueWorkflowCompileValidationError):  # order in presort_is matters
        validate_partition_spec(spec, to_validation_rules({"presort_is": "e,d desc"}))


def test_validate_input_schema():
    schema = "a:int,b:int,c:str"

    validate_input_schema(schema, to_validation_rules({"input_has": "a, c"}))
    validate_input_schema(schema, to_validation_rules({"input_has": "a, c,b:int"}))
    with raises(FugueWorkflowRuntimeValidationError):  # col type matters (if specified)
        validate_input_schema(schema, to_validation_rules({"input_has": "a, c,b:str"}))

    validate_input_schema(
        schema, to_validation_rules({"input_is": "a:int, b:int, c:str"})
    )
    with raises(FugueWorkflowRuntimeValidationError):  # col type matters (if specified)
        validate_input_schema(
            schema, to_validation_rules({"input_is": "a:int, b:int, c:int"})
        )
    with raises(FugueWorkflowRuntimeValidationError):  # order matters
        validate_input_schema(
            schema, to_validation_rules({"input_is": "a:int, c:str,b:int"})
        )
    with raises(SyntaxError):  # order matters
        validate_input_schema(schema, to_validation_rules({"input_is": "a,b,c"}))
