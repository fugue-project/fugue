from typing import Any, Callable, Dict
import ibis
import ibis.expr.datatypes as dt
import pyarrow as pa
from triad.utils.pyarrow import TRIAD_DEFAULT_TIMESTAMP
from triad import Schema, extensible_class

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

_IBIS_TO_PYARROW: Dict[dt.DataType, pa.DataType] = {
    dt.boolean: pa.bool_(),
    dt.int8: pa.int8(),
    dt.uint8: pa.uint8(),
    dt.int16: pa.int16(),
    dt.uint16: pa.uint16(),
    dt.int32: pa.int32(),
    dt.uint32: pa.uint32(),
    dt.int64: pa.int64(),
    dt.uint64: pa.uint64(),
    dt.float32: pa.float32(),
    dt.float64: pa.float64(),
    dt.string: pa.string(),
    dt.binary: pa.binary(),
    dt.timestamp: TRIAD_DEFAULT_TIMESTAMP,
    dt.date: pa.date32(),
}

_PYARROW_TO_IBIS: Dict[pa.DataType, dt.DataType] = {
    v: k for k, v in _IBIS_TO_PYARROW.items()
}


def to_ibis_schema(schema: Schema) -> ibis.Schema:
    fields = [(f.name, _pa_to_ibis_type(f.type)) for f in schema.fields]
    return ibis.schema(fields)


def to_schema(schema: ibis.Schema) -> Schema:
    fields = [(n, _ibis_to_pa_type(t)) for n, t in zip(schema.names, schema.types)]
    return Schema(fields)


def _ibis_to_pa_type(tp: dt.DataType) -> pa.DataType:
    if tp in _IBIS_TO_PYARROW:
        return _IBIS_TO_PYARROW[tp]
    if isinstance(tp, dt.Array):
        ttp = _ibis_to_pa_type(tp.value_type)
        return pa.list_(ttp)
    if isinstance(tp, dt.Struct):
        fields = [pa.field(n, _ibis_to_pa_type(t)) for n, t in zip(tp.names, tp.types)]
        return pa.struct(fields)
    raise NotImplementedError(tp)  # pragma: no cover


def _pa_to_ibis_type(tp: pa.DataType) -> dt.DataType:
    if tp in _PYARROW_TO_IBIS:
        return _PYARROW_TO_IBIS[tp]
    if pa.types.is_list(tp):
        ttp = _pa_to_ibis_type(tp.value_type)
        return dt.Array(ttp)
    if pa.types.is_struct(tp):
        fields = [(f.name, _pa_to_ibis_type(f.type)) for f in tp]
        return dt.Struct([x[0] for x in fields], [x[1] for x in fields])
    raise NotImplementedError(tp)  # pragma: no cover


def materialize(obj: "LazyIbisObject", materialize_func: Callable) -> Any:
    ctx = {k: materialize_func(v) for k, v in obj._super_lazy_internal_ctx.items()}
    return _materialize(obj, ctx)


@extensible_class
class LazyIbisObject:
    def __init__(self, obj: Any = None):
        self._super_lazy_internal_ctx: Dict[int, Any] = {}
        if obj is not None:
            self._super_lazy_internal_ctx[id(self)] = obj

    def __getattr__(self, name: str) -> Any:
        if not name.startswith("_"):
            return LazyIbisAttr(self, name)


class LazyIbisAttr(LazyIbisObject):
    def __init__(self, parent: LazyIbisObject, name: str):
        super().__init__()
        self._super_lazy_internal_ctx.update(parent._super_lazy_internal_ctx)
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
        super().__init__()
        self._super_lazy_internal_ctx.update(obj._super_lazy_internal_ctx)
        for x in args:
            if isinstance(x, LazyIbisObject):
                self._super_lazy_internal_ctx.update(x._super_lazy_internal_ctx)
        for x in kwargs.values():
            if isinstance(x, LazyIbisObject):
                self._super_lazy_internal_ctx.update(x._super_lazy_internal_ctx)
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


def _materialize(obj: Any, context: Dict[int, Any]) -> Any:
    if id(obj) in context:
        return context[id(obj)]
    if isinstance(obj, list):
        v: Any = [_materialize(x, context) for x in obj]
    elif isinstance(obj, tuple):
        v = tuple(_materialize(x, context) for x in obj)
    elif isinstance(obj, dict):
        v = {k: _materialize(v, context) for k, v in obj.items()}
    elif isinstance(obj, LazyIbisFunction):
        f = getattr(
            _materialize(obj._super_lazy_internal_objs["obj"], context),
            obj._super_lazy_internal_objs["func"],
        )
        v = f(
            *_materialize(obj._super_lazy_internal_objs["args"], context),
            **_materialize(obj._super_lazy_internal_objs["kwargs"], context)
        )
    elif isinstance(obj, LazyIbisAttr):
        v = getattr(
            _materialize(obj._super_lazy_internal_objs["parent"], context),
            obj._super_lazy_internal_objs["name"],
        )
    elif isinstance(obj, LazyIbisObject):
        raise NotImplementedError  # pragma: no cover
    else:
        v = obj
    context[id(obj)] = v
    return v
