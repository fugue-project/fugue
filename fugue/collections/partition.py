import json
from typing import Any, Dict, List

from triad.collections.dict import IndexedOrderedDict, ParamDict
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw as aot
from triad.utils.convert import to_size
from triad.utils.pyarrow import SchemaedDataPartitioner


class PartitionSpec(object):
    def __init__(self, *args: Any, **kwargs: Any):
        p = ParamDict()
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
        self._partition_by = p.get("partition_by", [])
        aot(
            len(self._partition_by) == len(set(self._partition_by)),
            SyntaxError(f"{self._partition_by} has duplicated keys"),
        )
        self._presort = self._parse_presort_exp(p.get_or_none("presort", object))
        if any(x in self._presort for x in self._partition_by):
            raise SyntaxError(
                "partition by overlap with presort: "
                + f"{self._partition_by}, {self._presort}"
            )
        # TODO: currently, size limit not in use
        self._size_limit = to_size(p.get("size_limit", "0"))
        self._row_limit = p.get("row_limit", 0)

    @property
    def empty(self) -> bool:
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
        return self._num_partitions

    def get_num_partitions(self, **expr_map_funcs: Any) -> int:
        expr = self.num_partitions
        for k, v in expr_map_funcs.items():
            if k in expr:
                value = str(v())
                expr = expr.replace(k, value)
        return int(eval(expr))

    @property
    def algo(self) -> str:
        return self._algo

    @property
    def partition_by(self) -> List[str]:
        return self._partition_by

    @property
    def presort(self) -> IndexedOrderedDict[str, bool]:
        return self._presort

    @property
    def presort_expr(self) -> str:
        return ",".join(
            k + " " + ("ASC" if v else "DESC") for k, v in self.presort.items()
        )

    @property
    def jsondict(self) -> ParamDict:
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

    def get_sorts(self, schema: Schema) -> IndexedOrderedDict[str, bool]:
        d: IndexedOrderedDict[str, bool] = IndexedOrderedDict()
        for p in self.partition_by:
            aot(p in schema, KeyError(f"{p} not in {schema}"))
            d[p] = True
        for p, v in self.presort.items():
            aot(p in schema, KeyError(f"{p} not in {schema}"))
            d[p] = v
        return d

    def get_key_schema(self, schema: Schema) -> Schema:
        return schema.extract(self.partition_by)

    def get_cursor(
        self, schema: Schema, physical_partition_no: int
    ) -> "PartitionCursor":
        return PartitionCursor(schema, self, physical_partition_no)

    def get_partitioner(self, schema: Schema) -> SchemaedDataPartitioner:
        pos = [schema.index_of_key(key) for key in self.partition_by]
        return SchemaedDataPartitioner(
            schema.pa_schema,
            pos,
            sizer=None,
            row_limit=self._row_limit,
            size_limit=self._size_limit,
        )

    def _parse_presort_exp(  # noqa: C901
        self, presort: Any
    ) -> IndexedOrderedDict[str, bool]:
        if presort is None:
            presort = ""
        if not isinstance(presort, str):
            return IndexedOrderedDict(presort)
        presort = presort.strip()
        res: IndexedOrderedDict[str, bool] = IndexedOrderedDict()
        if presort == "":
            return res
        for p in presort.split(","):
            pp = p.strip().split()
            key = pp[0].strip()
            if len(pp) == 1:
                value = True
            elif len(pp) == 2:
                if pp[1].strip().lower() == "asc":
                    value = True
                elif pp[1].strip().lower() == "desc":
                    value = False
                else:
                    raise SyntaxError(f"Invalid expression {presort}")
            else:
                raise SyntaxError(f"Invalid expression {presort}")
            if key in res:
                raise SyntaxError(f"Invalid expression {presort} duplicated key {key}")
            res[key] = value
        return res

    def _update_dict(self, d: Dict[str, Any], u: Dict[str, Any]) -> None:
        for k, v in u.items():
            if k == "by":
                k = "partition_by"
            if k == "num":
                k = "num_partitions"
            d[k] = v


EMPTY_PARTITION_SPEC = PartitionSpec()


class PartitionCursor(object):
    def __init__(self, schema: Schema, spec: PartitionSpec, physical_partition_no: int):
        self._orig_schema = schema
        self._key_index = [schema.index_of_key(key) for key in spec.partition_by]
        self._schema = schema.extract(spec.partition_by)
        self._physical_partition_no = physical_partition_no
        # the following will be set by the framework
        self._row: List[Any] = []
        self._partition_no = 0
        self._slice_no = 0

    def set(self, row: Any, partition_no: int, slice_no: int):
        self._row = list(row)
        self._partition_no = partition_no
        self._slice_no = slice_no

    @property
    def row(self) -> List[Any]:
        return self._row

    @property
    def partition_no(self) -> int:
        return self._partition_no

    @property
    def physical_partition_no(self) -> int:
        return self._physical_partition_no

    @property
    def slice_no(self) -> int:
        return self._slice_no

    @property
    def row_schema(self) -> Schema:
        return self._orig_schema

    @property
    def key_schema(self) -> Schema:
        return self._schema

    @property
    def key_value_dict(self) -> Dict[str, Any]:
        return {self.row_schema.names[i]: self._row[i] for i in self._key_index}

    @property
    def key_value_array(self) -> List[Any]:
        return [self._row[i] for i in self._key_index]

    def __getitem__(self, key: str) -> Any:
        return self._row[self.row_schema.index_of_key(key)]
