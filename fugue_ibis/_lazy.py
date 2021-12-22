from typing import Any, Dict

_SPECIAL_METHODS = [
    "__getitem__",
    "__add__",
    "__bool__",
    "__div__",
    "__eq__",
    "__floordiv__",
    "__ge__",
    "__gt__",
    "__hash__",
    "__le__",
    "__lt__",
    "__mod__",
    "__mul__",
    "__ne__",
    "__neg__",
    "__pow__",
    "__radd__",
    "__rdiv__",
    "__rfloordiv__",
    "__rmod__",
    "__rmul__",
    "__rpow__",
    "__rsub__",
    "__rtruediv__",
    "__sub__",
    "__truediv__",
]


class LazyIbisObject:
    def __getattr__(self, name: str) -> Any:
        if not name.startswith("_"):
            return LazyIbisAttr(self, name)


class LazyIbisAttr(LazyIbisObject):
    def __init__(self, parent: LazyIbisObject, name: str):
        self._super_lazy_internal_objs: Dict[str, Any] = dict(parent=parent, name=name)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return LazyIbisFunction(
            self._super_lazy_internal_objs["parent"],
            self._super_lazy_internal_objs["name"],
            *args,
            **kwargs
        )


class LazyIbisFunction(LazyIbisObject):
    def __init__(self, obj: LazyIbisObject, func: str, *args: Any, **kwargs: Any):
        self._super_lazy_internal_objs: Dict[str, Any] = dict(
            obj=obj, func=func, args=args, kwargs=kwargs
        )


def _wrapper(func: str):
    return lambda obj, *args, **kwargs: LazyIbisFunction(obj, func, *args, **kwargs)


for _method in _SPECIAL_METHODS:
    setattr(
        LazyIbisObject,
        _method,
        _wrapper(_method),
    )


def materialize(obj: Any, context: Dict[int, Any]) -> Any:
    if id(obj) in context:
        return context[id(obj)]
    if isinstance(obj, list):
        v: Any = [materialize(x, context) for x in obj]
    elif isinstance(obj, tuple):
        v = tuple([materialize(x, context) for x in obj])
    elif isinstance(obj, dict):
        v = {k: materialize(v, context) for k, v in obj.items()}
    elif isinstance(obj, LazyIbisFunction):
        f = getattr(
            materialize(obj._super_lazy_internal_objs["obj"], context),
            obj._super_lazy_internal_objs["func"],
        )
        v = f(
            *materialize(obj._super_lazy_internal_objs["args"], context),
            **materialize(obj._super_lazy_internal_objs["kwargs"], context)
        )
    elif isinstance(obj, LazyIbisAttr):
        v = getattr(
            materialize(obj._super_lazy_internal_objs["parent"], context),
            obj._super_lazy_internal_objs["name"],
        )
    elif isinstance(obj, LazyIbisObject):
        raise NotImplementedError  # pragma: no cover
    else:
        v = obj
    context[id(obj)] = v
    return v
