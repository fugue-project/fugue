from typing import Any, Dict, Iterable, List, Optional, Union

import pyarrow as pa
from triad import Schema, assert_or_throw, to_uuid
from triad.utils.pyarrow import _type_to_expression, to_pa_datatype


class ColumnExpr:
    """Fugue column expression class. It is inspired from
    :class:`spark:pyspark.sql.Column` and it is working in progress.

    .. admonition:: New Since
            :class: hint

            **0.6.0**

    .. caution::

        This is a base class of different column classes, and users are not supposed
        to construct this class directly. Use :func:`~.col` and :func:`~.lit` instead.
    """

    def __init__(self):
        self._as_name = ""
        self._as_type: Optional[pa.DataType] = None

    @property
    def name(self) -> str:
        """The original name of this column, default empty

        :return: the name

        .. admonition:: Examples

            .. code-block:: python

                assert "a" == col("a").name
                assert "b" == col("a").alias("b").name
                assert "" == lit(1).name
                assert "" == (col("a") * 2).name
        """
        return ""

    @property
    def as_name(self) -> str:
        """The name assigned by :meth:`~.alias`

        :return: the alias

        .. admonition:: Examples

            .. code-block:: python

                assert "" == col("a").as_name
                assert "b" == col("a").alias("b").as_name
                assert "x" == (col("a") * 2).alias("x").as_name
        """
        return self._as_name

    @property
    def as_type(self) -> Optional[pa.DataType]:
        """The type assigned by :meth:`~.cast`

        :return: the pyarrow datatype if :meth:`~.cast` was called
          otherwise None

        .. admonition:: Examples

            .. code-block:: python

                import pyarrow as pa

                assert col("a").as_type is None
                assert pa.int64() == col("a").cast(int).as_type
                assert pa.string() == (col("a") * 2).cast(str).as_type
        """
        return self._as_type

    @property
    def output_name(self) -> str:
        """The name assigned by :meth:`~.alias`, but if empty then
        return the original column name

        :return: the alias or the original column name

        .. admonition:: Examples

            .. code-block:: python

                assert "a" == col("a").output_name
                assert "b" == col("a").alias("b").output_name
                assert "x" == (col("a") * 2).alias("x").output_name
        """
        return self.as_name if self.as_name != "" else self.name

    def alias(self, as_name: str) -> "ColumnExpr":  # pragma: no cover
        """Assign or remove alias of a column. To remove, set ``as_name`` to empty

        :return: a new column with the alias value

        .. admonition:: Examples

            .. code-block:: python

                assert "b" == col("a").alias("b").as_name
                assert "x" == (col("a") * 2).alias("x").as_name
                assert "" == col("a").alias("b").alias("").as_name
        """
        raise NotImplementedError

    def infer_alias(self) -> "ColumnExpr":
        """Infer alias of a column. If the column's :meth:`~.output_name` is not empty
        then it returns itself without change. Otherwise it tries to infer alias from
        the underlying columns.

        :return: a column instance with inferred alias

        .. caution::

            Users should not use it directly.

        .. admonition:: Examples

            .. code-block:: python

                import fugue.column.functions as f

                assert "a" == col("a").infer_alias().output_name
                assert "" == (col("a") * 2).infer_alias().output_name
                assert "a" == col("a").is_null().infer_alias().output_name
                assert "a" == f.max(col("a").is_null()).infer_alias().output_name
        """
        return self

    def cast(self, data_type: Any) -> "ColumnExpr":  # pragma: no cover
        """Cast the column to a new data type

        :param data_type: It can be string expressions, python primitive types,
          python `datetime.datetime` and pyarrow types.
          For details read |FugueDataTypes|
        :return: a new column instance with the assigned data type

        .. caution::

            Currently, casting to struct or list type has undefined behavior.

        .. admonition:: Examples

            .. code-block:: python

                import pyarrow as pa

                assert pa.int64() == col("a").cast(int).as_type
                assert pa.string() == col("a").cast(str).as_type
                assert pa.float64() == col("a").cast(float).as_type
                assert pa._bool() == col("a").cast(bool).as_type

                # string follows the type expression of Triad Schema
                assert pa.int32() == col("a").cast("int").as_type
                assert pa.int32() == col("a").cast("int32").as_type

                assert pa.int32() == col("a").cast(pa.int32()).as_type
        """
        raise NotImplementedError

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        """Infer data type of this column given the input schema

        :param schema: the schema instance to infer from
        :return: a pyarrow datatype or None if failed to infer

        .. caution::

            Users should not use it directly.

        .. admonition:: Examples

            .. code-block:: python

                import pyarrow as pa
                from triad import Schema
                import fugue.column.functions as f

                schema = Schema("a:int,b:str")

                assert pa.int32() == col("a").infer_schema(schema)
                assert pa.int32() == (-col("a")).infer_schema(schema)
                # due to overflow risk, can't infer certain operations
                assert (col("a")+1).infer_schema(schema) is None
                assert (col("a")+col("a")).infer_schema(schema) is None
                assert pa.int32() == f.max(col("a")).infer_schema(schema)
                assert pa.int32() == f.min(col("a")).infer_schema(schema)
                assert f.sum(col("a")).infer_schema(schema) is None
        """
        return self.as_type  # pragma: no cover

    def __str__(self) -> str:
        """String expression of the column, this is only used for debug purpose.
        It is not SQL expression.

        :return: the string expression
        """
        res = self.body_str
        if self.as_type is not None:
            res = f"CAST({res} AS {_type_to_expression(self.as_type)})"
        if self.as_name != "":
            res = res + " AS " + self.as_name
        return res

    @property
    def body_str(self) -> str:  # pragma: no cover
        """The string expression of this column without cast type and alias.
        This is only used for debug purpose. It is not SQL expression.

        :return: the string expression
        """
        raise NotImplementedError

    def is_null(self) -> "ColumnExpr":
        """Same as SQL ``<col> IS NULL``.

        :return: a new column with the boolean values
        """
        # TODO: should enable infer_schema for this?
        return _UnaryOpExpr("IS_NULL", self)

    def not_null(self) -> "ColumnExpr":
        """Same as SQL ``<col> IS NOT NULL``.

        :return: a new column with the boolean values
        """
        # TODO: should enable infer_schema for this?
        return _UnaryOpExpr("NOT_NULL", self)

    def __neg__(self) -> "ColumnExpr":
        """The negative value of the current column

        :return: a new column with the negative value
        """
        return _InvertOpExpr("-", self)

    def __pos__(self) -> "ColumnExpr":
        """The original value of the current column

        :return: the column itself
        """
        return self

    def __invert__(self) -> "ColumnExpr":
        """Same as SQL ``NOT <col>``

        :return: a new column with the boolean values
        """
        return _NotOpExpr("~", self)

    def __add__(self, other: Any) -> "ColumnExpr":
        """Add with another column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BinaryOpExpr("+", self, other)

    def __radd__(self, other: Any) -> "ColumnExpr":
        """Add with another column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BinaryOpExpr("+", other, self)

    def __sub__(self, other: Any) -> "ColumnExpr":
        """Subtract another column from this column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BinaryOpExpr("-", self, other)

    def __rsub__(self, other: Any) -> "ColumnExpr":
        """Subtract this column from the other column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BinaryOpExpr("-", other, self)

    def __mul__(self, other: Any) -> "ColumnExpr":
        """Multiply with another column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BinaryOpExpr("*", self, other)

    def __rmul__(self, other: Any) -> "ColumnExpr":
        """Multiply with another column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BinaryOpExpr("*", other, self)

    def __truediv__(self, other: Any) -> "ColumnExpr":
        """Divide this column by the other column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BinaryOpExpr("/", self, other)

    def __rtruediv__(self, other: Any) -> "ColumnExpr":
        """Divide the other column by this column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BinaryOpExpr("/", other, self)

    def __and__(self, other: Any) -> "ColumnExpr":
        """``AND`` value of the two columns

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BoolBinaryOpExpr("&", self, other)

    def __rand__(self, other: Any) -> "ColumnExpr":
        """``AND`` value of the two columns

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BoolBinaryOpExpr("&", other, self)

    def __or__(self, other: Any) -> "ColumnExpr":
        """``OR`` value of the two columns

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BoolBinaryOpExpr("|", self, other)

    def __ror__(self, other: Any) -> "ColumnExpr":
        """``OR`` value of the two columns

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the result
        """
        return _BoolBinaryOpExpr("|", other, self)

    def __lt__(self, other: Any) -> "ColumnExpr":
        """Whether this column is less than the other column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the boolean result
        """
        return _BoolBinaryOpExpr("<", self, other)

    def __gt__(self, other: Any) -> "ColumnExpr":
        """Whether this column is greater than the other column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the boolean result
        """
        return _BoolBinaryOpExpr(">", self, other)

    def __le__(self, other: Any) -> "ColumnExpr":
        """Whether this column is less or equal to the other column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the boolean result
        """
        return _BoolBinaryOpExpr("<=", self, other)

    def __ge__(self, other: Any) -> "ColumnExpr":
        """Whether this column is greater or equal to the other column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the boolean result
        """
        return _BoolBinaryOpExpr(">=", self, other)

    def __eq__(self, other: Any) -> "ColumnExpr":  # type: ignore
        """Whether this column equals the other column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the boolean result
        """
        return _BoolBinaryOpExpr("==", self, other)

    def __ne__(self, other: Any) -> "ColumnExpr":  # type: ignore
        """Whether this column does not equal the other column

        :param other: the other column, if it is not a
          :class:`~.ColumnExpr`, then the value will be converted to
          a literal (`lit(other)`)
        :return: a new column with the boolean result
        """
        return _BoolBinaryOpExpr("!=", self, other)

    def __uuid__(self) -> str:
        """The unique id of this instance

        :return: the unique id
        """
        return to_uuid(
            str(type(self)),
            self.as_name,
            self.as_type,
            *self._uuid_keys(),
        )

    def _uuid_keys(self) -> List[Any]:  # pragma: no cover
        raise NotImplementedError


def lit(obj: Any, alias: str = "") -> ColumnExpr:
    """Convert the ``obj`` to a literal column. Currently ``obj`` must be
    ``int``, ``bool``, ``float`` or ``str``, otherwise an exception will
    be raised

    :param obj: an arbitrary value
    :param alias: the alias of this literal column, defaults to "" (no alias)
    :return: a literal column expression

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column import lit

            lit("abc")
            lit(100).alias("x")
            lit(100, "x")
    """
    return (
        _LiteralColumnExpr(obj) if alias == "" else _LiteralColumnExpr(obj).alias(alias)
    )


def null() -> ColumnExpr:
    """Equivalent to ``lit(None)``, the ``NULL`` value

    :return: ``lit(None)``

    .. admonition:: New Since
        :class: hint

        **0.6.0**
    """
    return lit(None)


def col(obj: Union[str, ColumnExpr], alias: str = "") -> ColumnExpr:
    """Convert the ``obj`` to a :class:`~.ColumnExpr` object

    :param obj: a string representing a column name or a :class:`~.ColumnExpr` object
    :param alias: the alias of this column, defaults to "" (no alias)
    :return: a literal column expression

    .. admonition:: New Since
        :class: hint

        **0.6.0**

    .. admonition:: Examples

        .. code-block:: python

            import fugue.column import col
            import fugue.column.functions as f

            col("a")
            col("a").alias("x")
            col("a", "x")

            # unary operations
            -col("a")  # negative value of a
            ~col("a")  # NOT a
            col("a").is_null()  # a IS NULL
            col("a").not_null()  # a IS NOT NULL

            # binary operations
            col("a") + 1  # col("a") + lit(1)
            1 - col("a")  # lit(1) - col("a")
            col("a") * col("b")
            col("a") / col("b")

            # binary boolean expressions
            col("a") == 1  # col("a") == lit(1)
            2 != col("a")  # col("a") != lit(2)
            col("a") < 5
            col("a") > 5
            col("a") <= 5
            col("a") >= 5
            (col("a") < col("b")) & (col("b") > 1) | col("c").is_null()

            # with functions
            f.max(col("a"))
            f.max(col("a")+col("b"))
            f.max(col("a")) + f.min(col("b"))
            f.count_distinct(col("a")).alias("dcount")

    """
    if isinstance(obj, ColumnExpr):
        return obj if alias == "" else obj.alias(alias)
    if isinstance(obj, str):
        return (
            _NamedColumnExpr(obj) if alias == "" else _NamedColumnExpr(obj).alias(alias)
        )
    raise NotImplementedError(obj)


def all_cols() -> ColumnExpr:
    """The ``*`` expression in SQL"""
    return _WildcardExpr()


def function(name: str, *args: Any, arg_distinct: bool = False, **kwargs) -> ColumnExpr:
    """Construct a function expression

    :param name: the name of the function
    :param arg_distinct: whether to add ``DISTINCT`` before all arguments,
      defaults to False
    :return: the function expression

    .. caution::

        Users should not use this directly

    """
    return _FuncExpr(name, *args, arg_distinct=arg_distinct, **kwargs)


def _get_column_mentions(column: ColumnExpr) -> Iterable[str]:
    if isinstance(column, _NamedColumnExpr):
        yield column.name
    elif isinstance(column, _FuncExpr):
        for a in column.args:
            yield from _get_column_mentions(a)
        for a in column.kwargs.values():
            yield from _get_column_mentions(a)


def _to_col(obj: Any) -> ColumnExpr:
    if isinstance(obj, ColumnExpr):
        return obj
    return lit(obj)


class _NamedColumnExpr(ColumnExpr):
    def __init__(self, name: Any):
        self._name = name
        super().__init__()

    @property
    def body_str(self) -> str:
        return self.name

    @property
    def name(self) -> str:
        return self._name

    @property
    def output_name(self) -> str:
        return super().output_name

    def alias(self, as_name: str) -> ColumnExpr:
        other = _NamedColumnExpr(self.name)
        other._as_name = as_name
        other._as_type = self.as_type
        return other

    def cast(self, data_type: Any) -> "ColumnExpr":
        other = _NamedColumnExpr(self.name)
        other._as_name = self.as_name
        other._as_type = None if data_type is None else to_pa_datatype(data_type)
        return other

    def infer_alias(self) -> ColumnExpr:
        if self.as_name == "" and self.as_type is not None:
            return self.alias(self.output_name)
        return self

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        if self.name not in schema:
            return self.as_type
        return self.as_type or schema[self.name].type

    def _uuid_keys(self) -> List[Any]:
        return [self.name]


class _WildcardExpr(ColumnExpr):
    @property
    def body_str(self) -> str:
        return "*"

    @property
    def name(self) -> str:  # pragma: no cover
        raise NotImplementedError("wildcard column doesn't have a name")

    @property
    def output_name(self) -> str:  # pragma: no cover
        raise NotImplementedError("wildcard column doesn't have an output_name")

    def __uuid__(self) -> str:
        """The unique id of this instance

        :return: the unique id
        """
        return to_uuid(str(type(self)))


class _LiteralColumnExpr(ColumnExpr):
    _VALID_TYPES = (int, bool, float, str)

    def __init__(self, value: Any):
        assert_or_throw(
            value is None or isinstance(value, _LiteralColumnExpr._VALID_TYPES),
            lambda: NotImplementedError(f"{value}, type: {type(value)}"),
        )
        self._value = value
        super().__init__()

    @property
    def body_str(self) -> str:
        if self.value is None:
            return "NULL"
        elif isinstance(self.value, str):
            body = self.value.translate(
                str.maketrans(
                    {  # type: ignore
                        "\\": r"\\",
                        "'": r"\'",
                    }
                )
            )
            return f"'{body}'"
        elif isinstance(self.value, bool):
            return "TRUE" if self.value else "FALSE"
        else:
            return str(self.value)

    @property
    def value(self) -> Any:
        return self._value

    def is_null(self) -> ColumnExpr:
        return _LiteralColumnExpr(self.value is None)

    def not_null(self) -> ColumnExpr:
        return _LiteralColumnExpr(self.value is not None)

    def alias(self, as_name: str) -> ColumnExpr:
        other = _LiteralColumnExpr(self.value)
        other._as_name = as_name
        other._as_type = self.as_type
        return other

    def cast(self, data_type: Any) -> ColumnExpr:
        other = _LiteralColumnExpr(self.value)
        other._as_name = self.as_name
        other._as_type = None if data_type is None else to_pa_datatype(data_type)
        return other

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        if self.value is None:
            return self.as_type
        return self.as_type or to_pa_datatype(type(self.value))

    def _uuid_keys(self) -> List[Any]:
        return [self.value]


class _FuncExpr(ColumnExpr):
    def __init__(
        self,
        func: str,
        *args: Any,
        arg_distinct: bool = False,
        **kwargs: Any,
    ):
        self._distinct = arg_distinct
        self._func = func
        self._args = list(args)
        self._kwargs = dict(kwargs)
        super().__init__()

    @property
    def body_str(self) -> str:
        def to_str(v: Any):
            if isinstance(v, str):
                return f"'{v}'"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            return str(v)

        a1 = [to_str(x) for x in self.args]
        a2 = [k + "=" + to_str(v) for k, v in self.kwargs.items()]
        args = ",".join(a1 + a2)
        distinct = "DISTINCT " if self.is_distinct else ""
        return f"{self.func}({distinct}{args})"

    @property
    def func(self) -> str:
        return self._func

    @property
    def is_distinct(self) -> bool:
        return self._distinct

    @property
    def args(self) -> List[Any]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, Any]:
        return self._kwargs

    def alias(self, as_name: str) -> ColumnExpr:
        other = self._copy()
        other._as_name = as_name
        other._distinct = self.is_distinct
        other._as_type = self.as_type
        return other

    def cast(self, data_type: Any) -> ColumnExpr:
        other = self._copy()
        other._as_name = self.as_name
        other._distinct = self.is_distinct
        other._as_type = None if data_type is None else to_pa_datatype(data_type)
        return other

    def _copy(self) -> "_FuncExpr":
        return _FuncExpr(self.func, *self.args, **self.kwargs)

    def _uuid_keys(self) -> List[Any]:
        return [self.func, self.is_distinct, self.args, self.kwargs]


class _UnaryOpExpr(_FuncExpr):
    def __init__(self, op: str, column: ColumnExpr, arg_distinct: bool = False):
        super().__init__(op, column, arg_distinct=arg_distinct)

    @property
    def col(self) -> ColumnExpr:
        return self.args[0]

    @property
    def op(self) -> str:
        return self.func

    def infer_alias(self) -> ColumnExpr:
        return (
            self
            if self.output_name != ""
            else self.alias(self.col.infer_alias().output_name)
        )

    def _copy(self) -> _FuncExpr:
        return _UnaryOpExpr(self.op, self.col)


class _InvertOpExpr(_UnaryOpExpr):
    def _copy(self) -> _FuncExpr:
        return _InvertOpExpr(self.op, self.col)

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        if self.as_type is not None:
            return self.as_type
        tp = self.col.infer_type(schema)
        if pa.types.is_signed_integer(tp) or pa.types.is_floating(tp):
            return tp
        return None


class _NotOpExpr(_UnaryOpExpr):
    def _copy(self) -> _FuncExpr:
        return _NotOpExpr(self.op, self.col)

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        if self.as_type is not None:
            return self.as_type
        tp = self.col.infer_type(schema)
        if pa.types.is_boolean(tp):
            return tp
        return None


class _BinaryOpExpr(_FuncExpr):
    def __init__(self, op: str, left: Any, right: Any, arg_distinct: bool = False):
        super().__init__(op, _to_col(left), _to_col(right), arg_distinct=arg_distinct)

    @property
    def left(self) -> ColumnExpr:
        return self.args[0]

    @property
    def right(self) -> ColumnExpr:
        return self.args[1]

    @property
    def op(self) -> str:
        return self.func

    def _copy(self) -> _FuncExpr:
        return _BinaryOpExpr(self.op, self.left, self.right)


class _BoolBinaryOpExpr(_BinaryOpExpr):
    def _copy(self) -> _FuncExpr:
        return _BoolBinaryOpExpr(self.op, self.left, self.right)

    def infer_type(self, schema: Schema) -> Optional[pa.DataType]:
        return self.as_type or pa.bool_()
