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
from antlr4.Token import CommonToken
from antlr4.tree.Tree import TerminalNode, Token, Tree
from fugue.collections.partition import PartitionSpec
from fugue.dataframe import DataFrames
from fugue.workflow.workflow import FugueWorkflow, WorkflowDataFrame
from fugue_sql._antlr import FugueSQLParser as fp
from fugue_sql._antlr import FugueSQLVisitor
from fugue_sql._parse import FugueSQL, _to_tokens
from fugue_sql.exceptions import FugueSQLError, FugueSQLSyntaxError
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import to_bool
from triad.utils.pyarrow import to_pa_datatype


class FugueSQLHooks(object):
    def on_select_source_not_found(
        self, workflow: FugueWorkflow, name: str
    ) -> Union[WorkflowDataFrame, str]:
        return name


class _VisitorBase(FugueSQLVisitor):
    def __init__(self, sql: FugueSQL):
        self._sql = sql

    @property
    def sql(self) -> FugueSQL:
        return self._sql

    def visitFugueIdentifier(self, ctx: fp.FugueIdentifierContext) -> str:
        return self.ctxToStr(ctx)

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

    def visitFugueLoadColumns(self, ctx: fp.FugueLoadColumnsContext) -> Any:
        if ctx.schema is not None:
            return str(self.visit(ctx.schema))
        else:
            return self.visit(ctx.cols)

    def visitFugueSaveMode(self, ctx: fp.FugueSaveModeContext) -> str:
        mode = self.ctxToStr(ctx).lower()
        if mode == "to":
            mode = "error"
        return mode

    def visitFugueFileFormat(self, ctx: fp.FugueFileFormatContext) -> str:
        return self.ctxToStr(ctx).lower()

    def visitFuguePath(self, ctx: fp.FuguePathContext) -> Any:
        return eval(self.ctxToStr(ctx))

    def visitFuguePersist(self, ctx: fp.FuguePersistContext) -> Any:
        if ctx.checkpoint:
            if ctx.ns:
                return True, self.visit(ctx.ns)
            return True, None
        else:
            if ctx.value is None:
                return False, None
            return False, self.visit(ctx.value)

    def visitFuguePersistValue(self, ctx: fp.FuguePersistValueContext) -> Any:
        return self.ctxToStr(ctx, delimit="")

    def visitFugueCheckpointNamespace(self, ctx: fp.FugueCheckpointNamespaceContext):
        return str(eval(self.ctxToStr(ctx)))


class _Extensions(_VisitorBase):
    def __init__(
        self,
        sql: FugueSQL,
        hooks: FugueSQLHooks,
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
        self._hooks = hooks

    @property
    def workflow(self) -> FugueWorkflow:
        return self._workflow

    @property
    def hooks(self) -> FugueSQLHooks:
        return self._hooks

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
        name = self.ctxToStr(ctx, delimit="")
        assert_or_throw(name in self.variables, FugueSQLError(f"{name} is not defined"))
        return self.variables[name]

    def visitFugueDataFrameNested(
        self, ctx: fp.FugueDataFrameNestedContext
    ) -> WorkflowDataFrame:
        sub = _Extensions(
            self.sql,
            self.hooks,
            workflow=self.workflow,
            variables=self.variables,
            last=self._last,
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
        data = self.get_dict(ctx, "partition", "dfs", "params")
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
        data = self.get_dict(ctx, "partition", "dfs", "params")
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
        data = self.get_dict(ctx, "params")
        p = data["params"]
        return self.workflow.create(
            using=p["using"], schema=p.get("schema"), params=p.get("params")
        )

    def visitFugueCreateDataTask(
        self, ctx: fp.FugueCreateDataTaskContext
    ) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "data", "schema")
        return self.workflow.df(data["data"], schema=data["schema"])

    def visitFugueZipTask(self, ctx: fp.FugueZipTaskContext) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "dfs", "how")
        partition_spec = PartitionSpec(**self.get_dict(ctx, "by", "presort"))
        # TODO: currently SQL does not support cache to file on ZIP
        return self.workflow.zip(
            data["dfs"], how=data.get("how", "inner"), partition=partition_spec
        )

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

    def visitFugueSaveTask(self, ctx: fp.FugueSaveTaskContext):
        data = self.get_dict(
            ctx, "partition", "df", "m", "single", "fmt", "path", "params"
        )
        if "df" in data:
            df = data["df"]
        else:
            df = self.last
        df.save(
            path=data["path"],
            fmt=data.get("fmt", ""),
            mode=data["m"],
            partition=data.get("partition"),
            single="single" in data,
            **data.get("params", {}),
        )

    def visitFugueLoadTask(self, ctx: fp.FugueLoadTaskContext) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "fmt", "path", "params", "columns")
        return self.workflow.load(
            path=data["path"],
            fmt=data.get("fmt", ""),
            columns=data.get("columns"),
            **data.get("params", {}),
        )

    def visitFugueNestableTask(self, ctx: fp.FugueNestableTaskContext) -> None:
        data = self.get_dict(ctx, "q")
        statements = list(self._beautify_sql(data["q"]))
        if len(statements) == 1 and isinstance(statements[0], WorkflowDataFrame):
            df: Any = statements[0]
        else:
            df = self.workflow.select(*statements)
        self._process_assignable(df, ctx)

    def visitQuery(self, ctx: fp.QueryContext) -> Iterable[Any]:
        return self._get_query_elements(ctx)

    def visitOptionalFromClause(
        self, ctx: fp.OptionalFromClauseContext
    ) -> Iterable[Any]:
        c = ctx.fromClause()
        if c is None:
            yield "FROM"
            yield self.last
        else:
            yield from self._get_query_elements(ctx)

    def visitTableName(self, ctx: fp.TableNameContext) -> Iterable[Any]:
        table_name = self.ctxToStr(ctx.multipartIdentifier(), delimit="")
        if table_name not in self.variables:
            table: Any = self.hooks.on_select_source_not_found(
                self.workflow, table_name
            )
        else:
            table = self.variables[table_name]
        if isinstance(table, str):
            yield table
            yield from self._get_query_elements(ctx.sample())
            yield from self._get_query_elements(ctx.tableAlias())
        else:
            yield table
            yield from self._get_query_elements(ctx.sample())
            if ctx.tableAlias().strictIdentifier() is not None:
                yield from self._get_query_elements(ctx.tableAlias())
            else:
                yield "AS"
                yield table_name

    def visitFugueNestableTaskCollectionNoSelect(
        self, ctx: fp.FugueNestableTaskCollectionNoSelectContext
    ) -> Iterable[Any]:
        last = self._last
        for i in range(ctx.getChildCount()):
            n = ctx.getChild(i)
            sub = _Extensions(
                self.sql,
                self.hooks,
                workflow=self.workflow,
                variables=self.variables,
                last=last,
            )
            yield sub.visit(n)

    def visitSetOperation(self, ctx: fp.SetOperationContext) -> Iterable[Any]:
        def get_sub(_ctx: Tree) -> List[Any]:

            sub = list(
                self.visitFugueTerm(_ctx)
                if isinstance(_ctx, fp.FugueTermContext)
                else self._get_query_elements(_ctx)
            )
            if len(sub) == 1 and isinstance(sub[0], WorkflowDataFrame):
                return ["SELECT * FROM", sub[0]]
            else:
                return sub

        yield from get_sub(ctx.left)
        yield from self._get_query_elements(ctx.operator)
        if ctx.setQuantifier() is not None:
            yield from self._get_query_elements(ctx.setQuantifier())
        yield from get_sub(ctx.right)

    def visitAliasedQuery(self, ctx: fp.AliasedQueryContext) -> Iterable[Any]:
        sub = list(self._get_query_elements(ctx.query()))
        if len(sub) == 1 and isinstance(sub[0], WorkflowDataFrame):
            yield sub[0]
        else:
            yield "("
            yield from sub
            yield ")"
        yield from self._get_query_elements(ctx.sample())
        yield from self._get_query_elements(ctx.tableAlias())

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
        if isinstance(node, CommonToken):
            yield self.sql.raw_code[node.start : node.stop + 1]
            return
        if isinstance(node, TerminalNode):
            token = node.getSymbol()
            yield self.sql.raw_code[token.start : token.stop + 1]
        for i in range(node.getChildCount()):
            n = node.getChild(i)
            if isinstance(n, fp.TableNameContext):
                yield from self.visitTableName(n)
            elif isinstance(n, fp.OptionalFromClauseContext):
                yield from self.visitOptionalFromClause(n)
            elif isinstance(n, fp.FugueTermContext):
                yield from self.visitFugueTerm(n)
            elif isinstance(n, fp.AliasedQueryContext):
                yield from self.visitAliasedQuery(n)
            elif isinstance(n, fp.SetOperationContext):
                yield from self.visitSetOperation(n)
            else:
                yield from self._get_query_elements(n)

    def _process_assignable(self, df: WorkflowDataFrame, ctx: Tree):
        data = self.get_dict(ctx, "assign", "persist", "broadcast")
        if "assign" in data:
            varname, sign = data["assign"]
        else:
            varname, sign = None, None
        need_checkpoint = sign == "??"
        if "persist" in data:
            is_checkpoint, value = data["persist"]
            if need_checkpoint or is_checkpoint:
                assert_or_throw(
                    is_checkpoint,
                    FugueSQLSyntaxError("can't persist when checkpoint is specified"),
                )
                df = df.checkpoint(value)
            else:
                df = df.persist(value)
        elif need_checkpoint:
            df = df.checkpoint()
        if "broadcast" in data:
            df = df.broadcast()
        if varname is not None:
            self.variables[varname] = df
        self._last = df
