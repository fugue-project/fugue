import json
from typing import Any, Dict, List, Tuple

from triad.collections.dict import IndexedOrderedDict, ParamDict
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw as aot
from triad.utils.convert import to_size
from triad.utils.pyarrow import SchemaedDataPartitioner
from triad.utils.hash import to_uuid


def parse_presort_exp(presort: Any) -> IndexedOrderedDict[str, bool]:  # noqa [C901]
    """Returns ordered column sorting direction where ascending order
    would return as true, and descending as false.

    :param presort: string that contains column and sorting direction or
        list of tuple that contains column and boolean sorting direction
    :type presort: Any

    :return: column and boolean sorting direction of column, order matters.
    :rtype: IndexedOrderedDict[str, bool]

    .. admonition:: Examples

        >>> parse_presort_exp("b desc, c asc")
        >>> parse_presort_exp([("b", True), ("c", False))])
        both return IndexedOrderedDict([("b", True), ("c", False))])
    """

    if isinstance(presort, IndexedOrderedDict):
        return presort

    presort_list: List[Tuple[str, bool]] = []
    res: IndexedOrderedDict[str, bool] = IndexedOrderedDict()
    if presort is None:
        return res

    elif isinstance(presort, str):
        presort = presort.strip()
        if presort == "":
            return res
        for p in presort.split(","):
            pp = p.strip().split()
            key = pp[0].strip()
            if len(pp) == 1:
                presort_list.append((key, True))
            elif len(pp) == 2:
                if pp[1].strip().lower() == "asc":
                    presort_list.append((key, True))
                elif pp[1].strip().lower() == "desc":
                    presort_list.append((key, False))
                else:
                    raise SyntaxError(f"Invalid expression {presort}")
            else:
                raise SyntaxError(f"Invalid expression {presort}")

    elif isinstance(presort, list):
        for p in presort:
            if isinstance(p, str):
                aot(
                    len(p.strip().split()) == 1,
                    SyntaxError(f"Invalid expression {presort}"),
                )
                presort_list.append((p.strip(), True))
            else:
                aot(len(p) == 2, SyntaxError(f"Invalid expression {presort}"))
                aot(
                    isinstance(p, tuple)
                    & (isinstance(p[0], str) & (isinstance(p[1], bool))),
                    SyntaxError(f"Invalid expression {presort}"),
                )
                presort_list.append((p[0].strip(), p[1]))

    for key, value in presort_list:
        if key in res:
            raise SyntaxError(f"Invalid expression {presort} duplicated key {key}")
        res[key] = value
    return res


class PartitionSpec(object):
    """Fugue Partition Specification.

    .. admonition:: Examples

        >>> PartitionSepc(num=4)
        >>> PartitionSepc(num="ROWCOUNT/4 + 3")  # It can be an expression
        >>> PartitionSepc(by=["a","b"])
        >>> PartitionSpec(by=["a"], presort="b DESC, c ASC")
        >>> PartitionSpec(algo="even", num=4)
        >>> p = PartitionSpec(num=4, by=["a"])
        >>> p_override = PartitionSpec(p, by=["a","b"], algo="even")
        >>> PartitionSpec(by="a")  # == PartitionSpec(by=["a"])
        >>> PartitionSpec("per_row")  # == PartitionSpec(num="ROWCOUNT", algo="even")

    It's important to understand this concept, please read |PartitionTutorial|

    Partition consists for these specs:

    * **algo**: can be one of ``hash`` (default), ``rand`` and ``even``
    * **num** or **num_partitions**: number of physical partitions, it can be an
      expression or integer numbers, e.g ``(ROWCOUNT+4) / 3``
    * **by** or **partition_by**: keys to partition on
    * **presort**: keys to sort other than partition keys. E.g. ``a``
      and ``a asc`` means presort by column a ascendingly, ``a,b desc``
      means presort by a ascendingly and then by b descendingly.
    * row_limit and size_limit are to be deprecated
    """

    def __init__(self, *args: Any, **kwargs: Any):  # noqa: C901
        p = ParamDict()
        if (
            len(args) == 1
            and len(kwargs) == 0
            and isinstance(args[0], str)
            and args[0].lower() == "per_row"
        ):
            p["algo"] = "even"
            p["num_partitions"] = "ROWCOUNT"
        else:
            for a in args:
                if a is None:
                    continue
                elif isinstance(a, PartitionSpec):
                    self._update_dict(p, a.jsondict)
                elif isinstance(a, Dict):
                    self._update_dict(p, a)
                elif isinstance(a, str):
                    self._update_dict(p, json.loads(a))
                else:
                    raise TypeError(f"{a} is not supported")
            self._update_dict(p, kwargs)
        self._num_partitions = p.get("num_partitions", "0")
        self._algo = p.get("algo", "").lower()
        if "partition_by" not in p:
            self._partition_by: List[str] = []
        elif isinstance(p["partition_by"], str):
            self._partition_by = [p["partition_by"]]
        elif isinstance(p["partition_by"], (list, tuple)):
            self._partition_by = list(p["partition_by"])
        else:
            raise SyntaxError(p["partition_by"])
        aot(
            len(self._partition_by) == len(set(self._partition_by)),
            SyntaxError(f"{self._partition_by} has duplicated keys"),
        )
        self._presort = parse_presort_exp(p.get_or_none("presort", object))
        if any(x in self._presort for x in self._partition_by):
            raise SyntaxError(
                "partition by overlap with presort: "
                + f"{self._partition_by}, {self._presort}"
            )
        # TODO: currently, size limit not in use
        self._size_limit = to_size(p.get("size_limit", "0"))
        self._row_limit = p.get("row_limit", 0)

    def __repr__(self) -> str:
        return (
            f"PartitionSpec(num='{self._num_partitions}', "
            + f"by={self._partition_by}, presort='{self.presort_expr}')"
        )

    def __eq__(self, other: Any) -> bool:
        if other is self:
            return True
        if not isinstance(other, PartitionSpec):
            other = PartitionSpec(other)
        return self.jsondict == other.jsondict

    @property
    def empty(self) -> bool:
        """Whether this spec didn't specify anything"""
        return (
            self._num_partitions == "0"
            and self._algo == ""
            and len(self._partition_by) == 0
            and len(self._presort) == 0
            and self._size_limit == 0
            and self._row_limit == 0
        )

    @property
    def num_partitions(self) -> str:
        """Number of partitions, it can be a string expression or int"""
        return self._num_partitions

    def get_num_partitions(self, **expr_map_funcs: Any) -> int:
        """Convert ``num_partitions`` expression to int number

        :param expr_map_funcs: lambda functions (no parameter) for keywords
        :return: integer value of the partitions

        .. admonition:: Examples

            >>> p = PartitionSpec(num="ROWCOUNT/2")
            >>> p.get_num_partitions(ROWCOUNT=lambda: df.count())
        """
        expr = self.num_partitions
        for k, v in expr_map_funcs.items():
            if k in expr:
                value = str(v())
                expr = expr.replace(k, value)
        return int(eval(expr))  # pylint: disable=W0123

    @property
    def algo(self) -> str:
        """Get algo of the spec, one of ``hash`` (default), ``rand`` and ``even``"""
        return self._algo if self._algo != "" else "hash"

    @property
    def partition_by(self) -> List[str]:
        """Get partition keys of the spec"""
        return self._partition_by

    @property
    def presort(self) -> IndexedOrderedDict[str, bool]:
        """Get presort pairs of the spec

        .. admonition:: Examples

            >>> p = PartitionSpec(by=["a"],presort="b,c desc")
            >>> assert p.presort == {"b":True, "c":False}
        """
        return self._presort

    @property
    def presort_expr(self) -> str:
        """Get normalized presort expression

        .. admonition:: Examples

            >>> p = PartitionSpec(by=["a"],presort="b , c dESc")
            >>> assert p.presort_expr == "b ASC,c DESC"
        """
        return ",".join(
            k + " " + ("ASC" if v else "DESC") for k, v in self.presort.items()
        )

    @property
    def jsondict(self) -> ParamDict:
        """Get json serializeable dict of the spec"""
        return ParamDict(
            dict(
                num_partitions=self._num_partitions,
                algo=self._algo,
                partition_by=self._partition_by,
                presort=self.presort_expr,
                size_limit=self._size_limit,
                row_limit=self._row_limit,
            )
        )

    def __uuid__(self) -> str:
        """Get deterministic unique id of this object"""
        return to_uuid(self.jsondict)

    def get_sorts(self, schema: Schema) -> IndexedOrderedDict[str, bool]:
        """Get keys for sorting in a partition, it's the combination of partition
        keys plus the presort keys

        :param schema: the dataframe schema this partition spec to operate on
        :return: an ordered dictionary of key, order pairs

        .. admonition:: Examples

            >>> p = PartitionSpec(by=["a"],presort="b , c dESc")
            >>> schema = Schema("a:int,b:int,c:int,d:int"))
            >>> assert p.get_sorts(schema) == {"a":True, "b":True, "c": False}
        """
        d: IndexedOrderedDict[str, bool] = IndexedOrderedDict()
        for p in self.partition_by:
            aot(p in schema, lambda: KeyError(f"{p} not in {schema}"))
            d[p] = True
        for p, v in self.presort.items():
            aot(p in schema, lambda: KeyError(f"{p} not in {schema}"))
            d[p] = v
        return d

    def get_key_schema(self, schema: Schema) -> Schema:
        """Get partition keys schema

        :param schema: the dataframe schema this partition spec to operate on
        :return: the sub-schema only containing partition keys
        """
        return schema.extract(self.partition_by)

    def get_cursor(
        self, schema: Schema, physical_partition_no: int
    ) -> "PartitionCursor":
        """Get :class:`.PartitionCursor` based on
        dataframe schema and physical partition number. You normally don't call
        this method directly

        :param schema: the dataframe schema this partition spec to operate on
        :param physical_partition_no: physical partition no passed in by
          :class:`~fugue.execution.execution_engine.ExecutionEngine`
        :return: PartitionCursor object
        """
        return PartitionCursor(schema, self, physical_partition_no)

    def get_partitioner(self, schema: Schema) -> SchemaedDataPartitioner:
        """Get :class:`~triad.utils.pyarrow.SchemaedDataPartitioner` by input
        dataframe schema

        :param schema: the dataframe schema this partition spec to operate on
        :return: SchemaedDataPartitioner object
        """
        pos = [schema.index_of_key(key) for key in self.partition_by]
        return SchemaedDataPartitioner(
            schema.pa_schema,
            pos,
            sizer=None,
            row_limit=self._row_limit,
            size_limit=self._size_limit,
        )

    def _update_dict(self, d: Dict[str, Any], u: Dict[str, Any]) -> None:
        for k, v in u.items():
            if k == "by":
                k = "partition_by"
            if k == "num":
                k = "num_partitions"
            d[k] = v


EMPTY_PARTITION_SPEC = PartitionSpec()


class PartitionCursor(object):
    """The cursor pointing at the first row of each logical partition inside
    a physical partition.

    It's important to understand the concept of partition, please read
    |PartitionTutorial|

    :param schema: input dataframe schema
    :param spec: partition spec
    :param physical_partition_no: physical partition number passed in by
      :class:`~fugue.execution.execution_engine.ExecutionEngine`
    """

    def __init__(self, schema: Schema, spec: PartitionSpec, physical_partition_no: int):
        self._orig_schema = schema
        self._key_index = [schema.index_of_key(key) for key in spec.partition_by]
        self._schema = schema.extract(spec.partition_by)
        self._physical_partition_no = physical_partition_no
        # the following will be set by the framework
        self._row: List[Any] = []
        self._partition_no = 0
        self._slice_no = 0

    def set(self, row: Any, partition_no: int, slice_no: int) -> None:
        """reset the cursor to a row (which should be the first row of a
        new logical partition)

        :param row: list-like row data
        :param partition_no: logical partition number
        :param slice_no: slice number inside the logical partition (to be deprecated)
        """
        self._row = list(row)
        self._partition_no = partition_no
        self._slice_no = slice_no

    @property
    def row(self) -> List[Any]:
        """Get current row data"""
        return self._row

    @property
    def partition_no(self) -> int:
        """Logical partition number"""
        return self._partition_no

    @property
    def physical_partition_no(self) -> int:
        """Physical partition number"""
        return self._physical_partition_no

    @property
    def slice_no(self) -> int:
        """Slice number (inside the current logical partition), for now
        it should always be 0
        """
        return self._slice_no

    @property
    def row_schema(self) -> Schema:
        """Schema of the current row"""
        return self._orig_schema

    @property
    def key_schema(self) -> Schema:
        """Partition key schema"""
        return self._schema

    @property
    def key_value_dict(self) -> Dict[str, Any]:
        """Based on current row, get the partition key values as a dict"""
        return {self.row_schema.names[i]: self._row[i] for i in self._key_index}

    @property
    def key_value_array(self) -> List[Any]:
        """Based on current row, get the partition key values as an array"""
        return [self._row[i] for i in self._key_index]

    def __getitem__(self, key: str) -> Any:
        """Get value by key from the current row

        :param key: column name
        :return: value in the column
        """
        return self._row[self.row_schema.index_of_key(key)]
