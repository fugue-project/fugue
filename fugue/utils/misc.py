from typing import Type, TypeVar

from triad.utils.assertion import assert_or_throw

T = TypeVar("T")


def get_attribute(obj: object, attr_name: str, data_type: Type[T]) -> T:
    if attr_name not in obj.__dict__ or obj.__dict__[attr_name] is None:
        obj.__dict__[attr_name] = data_type()
    assert_or_throw(
        isinstance(obj.__dict__[attr_name], data_type),
        TypeError(f"{obj.__dict__[attr_name]} is not type {data_type}"),
    )
    return obj.__dict__[attr_name]
