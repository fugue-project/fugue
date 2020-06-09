from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    no_type_check,
)

import pyarrow as pa
from antlr4.tree.Tree import TerminalNode, Token, Tree
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrames
from fugue.workflow.workflow import FugueWorkflow, WorkflowDataFrame
from fugue_sql.antlr import FugueSQLParser as fp
from fugue_sql.antlr import FugueSQLVisitor
from fugue_sql.exceptions import FugueSQLError, FugueSQLSyntaxError
from fugue_sql.parse import FugueSQL, _to_tokens
from triad.collections.schema import Schema
from triad.utils.convert import to_bool
from triad.utils.pyarrow import to_pa_datatype


class _VisitorBase(FugueSQLVisitor):
    def __init__(self, sql: FugueSQL):
        self._sql = sql

    @property
    def sql(self) -> FugueSQL:
        return self._sql

    def visitFugueIdentifier(self, ctx: fp.FugueIdentifierContext) -> str:
        return self.ctxToStr(ctx)

    # def visitTerminal(self, node):
    #    token = node.getSymbol()
    #    return self.sql.code[token.start: token.stop + 1]

    def collectChildren(self, node: Tree, tp: Type) -> List[Any]:
        result: List[Any] = []
        n = node.getChildCount()
        for i in range(n):
            c = node.getChild(i)
            if isinstance(c, tp):
                result.append(c.accept(self))
        return result

    def ctxToStr(self, node: Union[Tree, Token, None], delimit: str = " ") -> str:
        if isinstance(node, Token):
            tokens: Iterable[Token] = [node]
        else:
            tokens = _to_tokens(node)
        return delimit.join([self.sql.raw_code[t.start : t.stop + 1] for t in tokens])

    @no_type_check
    def to_kv(self, ctx: Tree) -> Tuple[Any, Any]:
        k = ctx.key.accept(self)
        v = ctx.value.accept(self)
        return (k, v)

    def get_dict(self, ctx: Tree, *keys: Any) -> Dict[str, Any]:
        res: Dict[str, Any] = {}
        for k in keys:
            v = getattr(ctx, k)
            if v is not None:
                res[k] = self.visit(v)
        return res

    def visitFugueJsonObj(self, ctx: fp.FugueJsonObjContext) -> Any:
        pairs = ctx.fugueJsonPairs()
        if pairs is None:
            return dict()
        return pairs.accept(self)

    def visitFugueJsonPairs(self, ctx: fp.FugueJsonPairsContext) -> Dict:
        return dict(self.collectChildren(ctx, fp.FugueJsonPairContext))

    def visitFugueJsonPair(self, ctx: fp.FugueJsonPairContext) -> Any:
        return self.to_kv(ctx)

    def visitFugueJsonArray(self, ctx: fp.FugueJsonArrayContext) -> List[Any]:
        return self.collectChildren(ctx, fp.FugueJsonValueContext)

    def visitFugueJsonString(self, ctx: fp.FugueJsonKeyContext) -> Any:
        return eval(self.ctxToStr(ctx))

    def visitFugueJsonNumber(self, ctx: fp.FugueJsonKeyContext) -> Any:
        return eval(self.ctxToStr(ctx))

    def visitFugueJsonBool(self, ctx: fp.FugueJsonKeyContext) -> bool:
        return to_bool(self.ctxToStr(ctx))

    def visitFugueJsonNull(self, ctx: fp.FugueJsonKeyContext) -> Any:
        return None

    def visitFugueWildSchema(self, ctx: fp.FugueWildSchemaContext) -> Schema:
        schema = ",".join(self.collectChildren(ctx, fp.FugueWildSchemaPairContext))
        if schema.count("*") > 1:
            raise FugueSQLSyntaxError(f"invalid {schema} * can appear at most once")
        return schema

    def visitFugueWildSchemaPair(self, ctx: fp.FugueWildSchemaPairContext) -> str:
        if ctx.pair is not None:
            return str(Schema([self.visit(ctx.pair)]))
        else:
            return "*"

    def visitFugueSchema(self, ctx: fp.FugueSchemaContext) -> Schema:
        return Schema(self.collectChildren(ctx, fp.FugueSchemaPairContext))

    def visitFugueSchemaPair(self, ctx: fp.FugueSchemaPairContext) -> Any:
        return self.to_kv(ctx)

    def visitFugueSchemaSimpleType(
        self, ctx: fp.FugueSchemaSimpleTypeContext
    ) -> pa.DataType:
        return to_pa_datatype(self.ctxToStr(ctx))

    def visitFugueSchemaListType(
        self, ctx: fp.FugueSchemaListTypeContext
    ) -> pa.DataType:
        tp = self.visit(ctx.fugueSchemaType())
        return pa.list_(tp)

    def visitFugueSchemaStructType(
        self, ctx: fp.FugueSchemaStructTypeContext
    ) -> pa.DataType:
        fields = self.visit(ctx.fugueSchema()).fields
        return pa.struct(fields)

    def visitFuguePrepartition(self, ctx: fp.FuguePrepartitionContext) -> PartitionSpec:
        params = self.get_dict(ctx, "algo", "num", "by", "presort")
        return PartitionSpec(**params)

    def visitFuguePartitionAlgo(self, ctx: fp.FuguePartitionAlgoContext) -> str:
        return self.ctxToStr(ctx).lower()

    def visitFuguePartitionNum(self, ctx: fp.FuguePartitionNumContext) -> str:
        return self.ctxToStr(ctx, delimit="").upper()

    def visitFugueCols(self, ctx: fp.FugueColsContext) -> List[str]:
        return self.collectChildren(ctx, fp.FugueColumnIdentifierContext)

    def visitFugueColsSort(self, ctx: fp.FugueColsSortContext) -> str:
        return ",".join(self.collectChildren(ctx, fp.FugueColSortContext))

    def visitFugueColSort(self, ctx: fp.FugueColSortContext) -> str:
        return self.ctxToStr(ctx)

    def visitFugueColumnIdentifier(self, ctx: fp.FugueColumnIdentifierContext) -> str:
        return self.ctxToStr(ctx)

    def visitFugueParamsPairs(self, ctx: fp.FugueParamsPairsContext) -> Dict:
        return dict(self.collectChildren(ctx.pairs, fp.FugueJsonPairContext))

    def visitFugueParamsObj(self, ctx: fp.FugueParamsObjContext) -> Any:
        return self.visit(ctx.obj)

    def visitFugueExtension(self, ctx: fp.FugueExtensionContext) -> str:
        return self.ctxToStr(ctx, delimit="")

    def visitFugueSingleOutputExtensionCommon(
        self, ctx: fp.FugueSingleOutputExtensionCommonContext
    ) -> Dict[str, Any]:
        return self.get_dict(ctx, "using", "params", "schema")

    def visitFugueSingleOutputExtensionCommonWild(
        self, ctx: fp.FugueSingleOutputExtensionCommonContext
    ) -> Dict[str, Any]:
        return self.get_dict(ctx, "using", "params", "schema")

    def visitFugueAssignment(self, ctx: fp.FugueAssignmentContext) -> Tuple:
        varname = self.ctxToStr(ctx.varname, delimit="")
        sign = self.ctxToStr(ctx.sign, delimit="")
        return varname, sign

    def visitFugueZipType(self, ctx: fp.FugueZipTypeContext) -> str:
        return self.ctxToStr(ctx, delimit="_").lower()


class _Extensions(_VisitorBase):
    def __init__(
        self,
        sql: FugueSQL,
        workflow: FugueWorkflow,
        variables: Optional[Dict[str, WorkflowDataFrame]] = None,
        last: Optional[WorkflowDataFrame] = None,
    ):
        super().__init__(sql)
        self._workflow = workflow
        self._variables: Dict[str, WorkflowDataFrame] = {}
        if variables is not None:
            self._variables.update(variables)
        self._last: Optional[WorkflowDataFrame] = last

    @property
    def workflow(self) -> FugueWorkflow:
        return self._workflow

    @property
    def variables(self) -> Dict[str, WorkflowDataFrame]:
        return self._variables

    @property
    def last(self) -> WorkflowDataFrame:
        if self._last is None:
            raise FugueSQLError("latest dataframe does not exist")
        return self._last

    def visitFugueDataFrameSource(
        self, ctx: fp.FugueDataFrameSourceContext
    ) -> WorkflowDataFrame:
        return self.variables[self.ctxToStr(ctx, delimit="")]

    def visitFugueDataFrameNested(
        self, ctx: fp.FugueDataFrameNestedContext
    ) -> WorkflowDataFrame:
        sub = _Extensions(
            self.sql, workflow=self.workflow, variables=self.variables, last=self._last
        )
        sub.visit(ctx.task)
        return sub.last

    def visitFugueDataFramePair(self, ctx: fp.FugueDataFramePairContext) -> Any:
        return self.to_kv(ctx)

    def visitFugueDataFramesList(
        self, ctx: fp.FugueDataFramesListContext
    ) -> DataFrames:
        dfs = self.collectChildren(ctx, fp.FugueDataFrameContext)
        return DataFrames(dfs)

    def visitFugueDataFramesDict(
        self, ctx: fp.FugueDataFramesDictContext
    ) -> DataFrames:
        dfs = self.collectChildren(ctx, fp.FugueDataFramePairContext)
        return DataFrames(dfs)

    def visitFugueTransformTask(
        self, ctx: fp.FugueTransformTaskContext
    ) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "partition", "dfs", "params", "persist", "broadcast")
        if "dfs" not in data:
            data["dfs"] = DataFrames(self.last)
        p = data["params"]
        # ignore errors is not implemented
        return self.workflow.transform(
            data["dfs"],
            using=p["using"],
            schema=p.get("schema"),
            params=p.get("params"),
            pre_partition=data.get("partition"),
        )

    def visitFugueProcessTask(
        self, ctx: fp.FugueProcessTaskContext
    ) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "partition", "dfs", "params", "persist", "broadcast")
        if "dfs" not in data:
            data["dfs"] = DataFrames(self.last)
        p = data["params"]
        return self.workflow.process(
            data["dfs"],
            using=p["using"],
            schema=p.get("schema"),
            params=p.get("params"),
            pre_partition=data.get("partition"),
        )

    def visitFugueCreateTask(self, ctx: fp.FugueCreateTaskContext) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "params", "persist", "broadcast")
        p = data["params"]
        return self.workflow.create(
            using=p["using"], schema=p.get("schema"), params=p.get("params")
        )

    def visitFugueCreateDataTask(
        self, ctx: fp.FugueCreateDataTaskContext
    ) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "data", "schema", "persist", "broadcast")
        return self.workflow.df(data["data"], schema=data["schema"])

    def visitFugueZipTask(self, ctx: fp.FugueZipTaskContext):
        data = self.get_dict(ctx, "dfs", "how", "persist", "broadcast")
        partition_spec = PartitionSpec(**self.get_dict(ctx, "by", "presort"))
        # TODO: currently SQL does not support cache to file on ZIP
        return self.workflow.zip(
            data["dfs"], how=data.get("how", "inner"), partition=partition_spec
        )

    def visitFugueNestableTaskNoSelect(
        self, ctx: fp.FugueNestableTaskNoSelectContext
    ) -> None:
        data = self.get_dict(ctx, "assign", "df")
        if "assign" in data:
            self.variables[data["assign"][0]] = data["df"]
        self._last = data["df"]

    def visitFugueOutputTask(self, ctx: fp.FugueOutputTaskContext):
        data = self.get_dict(ctx, "dfs", "using", "params", "partition")
        if "dfs" not in data:
            data["dfs"] = DataFrames(self.last)
        self.workflow.output(
            data["dfs"],
            using=data["using"],
            params=data.get("params"),
            pre_partition=data.get("partition"),
        )

    def visitFuguePrintTask(self, ctx: fp.FuguePrintTaskContext) -> None:
        data = self.get_dict(ctx, "dfs")
        if "dfs" not in data:
            data["dfs"] = DataFrames(self.last)
        params: Dict[str, Any] = {}
        if ctx.rows is not None:
            params["rows"] = int(self.ctxToStr(ctx.rows))
        if ctx.count is not None:
            params["show_count"] = True
        if ctx.title is not None:
            params["title"] = eval(self.ctxToStr(ctx.title))
        self.workflow.show(data["dfs"], **params)

    def visitFugueSelectTask(self, ctx: fp.FugueSelectTaskContext) -> None:
        data = self.get_dict(ctx, "assign", "partition", "q", "persist", "broadcast")
        statements = list(self._beautify_sql(data["q"]))
        # print(statements, self.ctxToStr(ctx), "-")
        df = self.workflow.select(*statements)
        if "assign" in data:
            self.variables[data["assign"][0]] = df
        self._last = df

    def visitQuery(self, ctx: fp.QueryContext) -> Iterable[Any]:
        return self._get_query_elements(ctx)

    def visitTableName(self, ctx: fp.TableNameContext) -> Iterable[Any]:
        table_name = self.ctxToStr(ctx.multipartIdentifier(), delimit="")
        if table_name not in self.variables:
            yield table_name
            for x in self._get_query_elements(ctx.sample()):
                yield x
            for x in self._get_query_elements(ctx.tableAlias()):
                yield x
        else:
            yield self.variables[table_name]
            for x in self._get_query_elements(ctx.sample()):
                yield x
            if ctx.tableAlias().strictIdentifier() is not None:
                for x in self._get_query_elements(ctx.tableAlias()):
                    yield x
            else:
                yield "AS"
                yield table_name

    def visitAliasedFugueNested(
        self, ctx: fp.AliasedFugueNestedContext
    ) -> Iterable[Any]:
        sub = _Extensions(
            self.sql, workflow=self.workflow, variables=self.variables, last=self._last
        )
        sub.visit(ctx.fugueNestableTaskNoSelect())
        yield sub.last
        for x in self._get_query_elements(ctx.sample()):
            yield x
        for x in self._get_query_elements(ctx.tableAlias()):
            yield x

    def _beautify_sql(self, statements: Iterable[Any]) -> Iterable[Any]:
        current = ""
        for s in statements:
            if not isinstance(s, str):
                if current != "":
                    yield current
                yield s
                current = ""
            else:
                s = s.strip()
                if s != "":
                    if (
                        current == ""
                        or current.endswith(".")
                        or s.startswith(".")
                        or current.endswith("(")
                        or s.startswith(")")
                    ):
                        current += s
                    else:
                        current += " " + s
        if current != "":
            yield current

    def _get_query_elements(self, node: Tree) -> Iterable[Any]:  # noqa: C901
        if node is None:
            return
        if isinstance(node, TerminalNode):
            token = node.getSymbol()
            yield self.sql.raw_code[token.start : token.stop + 1]
        for i in range(node.getChildCount()):
            n = node.getChild(i)
            if isinstance(n, fp.TableNameContext):
                for x in self.visitTableName(n):
                    yield x
            elif isinstance(n, fp.AliasedFugueNestedContext):
                for x in self.visitAliasedFugueNested(n):
                    yield x
            elif isinstance(n, fp.QueryContext):
                for x in self.visitQuery(n):
                    yield x
            else:
                for x in self._get_query_elements(n):
                    yield x
