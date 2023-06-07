from typing import Any, Type, TypeVar

from triad.utils.assertion import assert_or_throw

T = TypeVar("T")


def get_attribute(obj: object, attr_name: str, data_type: Type[T]) -> T:
    if attr_name not in obj.__dict__ or obj.__dict__[attr_name] is None:
        obj.__dict__[attr_name] = data_type()
    assert_or_throw(
        isinstance(obj.__dict__[attr_name], data_type),
        lambda: TypeError(f"{obj.__dict__[attr_name]} is not type {data_type}"),
    )
    return obj.__dict__[attr_name]


def import_or_throw(package_name: str, message: str) -> Any:
    try:
        return __import__(package_name)
    except Exception as e:  # pragma: no cover
        raise ImportError(str(e) + ". " + message)


def import_fsql_dependency(package_name: str) -> Any:
    return import_or_throw(
        package_name, "Please try to install the package by `pip install fugue[sql]`."
    )
