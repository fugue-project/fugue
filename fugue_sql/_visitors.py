# pylint: disable-all

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
from antlr4 import ParserRuleContext

import pyarrow as pa
from antlr4.Token import CommonToken
from antlr4.tree.Tree import TerminalNode, Token, Tree
from fugue import (
    FugueWorkflow,
    PartitionSpec,
    SQLEngine,
    WorkflowDataFrame,
    WorkflowDataFrames,
)
from fugue.extensions.creator.convert import _to_creator
from fugue.extensions.outputter.convert import _to_outputter
from fugue.extensions.processor.convert import _to_processor
from fugue.extensions.transformer.convert import _to_output_transformer, _to_transformer
from fugue.workflow.module import _to_module
from triad import to_uuid
from triad.collections.schema import Schema
from triad.utils.assertion import assert_or_throw
from triad.utils.convert import (
    get_caller_global_local_vars,
    to_bool,
    to_function,
    to_type,
)
from triad.utils.pyarrow import to_pa_datatype
from triad.utils.string import validate_triad_var_name

from fugue_sql._antlr import FugueSQLParser as fp
from fugue_sql._antlr import FugueSQLVisitor
from fugue_sql._parse import FugueSQL, _to_tokens
from fugue_sql._utils import LazyWorkflowDataFrame
from fugue_sql.exceptions import (
    FugueSQLError,
    FugueSQLRuntimeError,
    FugueSQLSyntaxError,
)


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

    def to_runtime_error(self, ctx: ParserRuleContext) -> Exception:
        interval = ctx.getSourceInterval()
        msg = "\n" + self.sql.get_raw_lines(interval[0], interval[1], add_lineno=True)
        return FugueSQLRuntimeError(msg)

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

    def visitFugueRenamePair(self, ctx: fp.FugueRenamePairContext) -> Tuple:
        return self.to_kv(ctx)

    def visitFugueRenameExpression(
        self, ctx: fp.FugueRenameExpressionContext
    ) -> Dict[str, str]:
        return dict(self.collectChildren(ctx, fp.FugueRenamePairContext))

    def visitFugueWildSchema(self, ctx: fp.FugueWildSchemaContext) -> str:
        schema = ",".join(self.collectChildren(ctx, fp.FugueWildSchemaPairContext))
        if schema.count("*") > 1:
            raise FugueSQLSyntaxError(f"invalid {schema} * can appear at most once")
        ops = "".join(self.collectChildren(ctx, fp.FugueSchemaOpContext))
        return schema + ops

    def visitFugueWildSchemaPair(self, ctx: fp.FugueWildSchemaPairContext) -> str:
        if ctx.pair is not None:
            return str(Schema([self.visit(ctx.pair)]))
        else:
            return "*"

    def visitFugueSchemaOp(self, ctx: fp.FugueSchemaOpContext) -> str:
        return self.ctxToStr(ctx, delimit="")

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

    def visitFugueSampleMethod(self, ctx: fp.FugueSampleMethodContext) -> Tuple:
        if ctx.rows is not None:
            n: Any = int(self.ctxToStr(ctx.rows))
        else:
            n = None
        if ctx.percentage is not None:
            frac: Any = float(self.ctxToStr(ctx.percentage)) / 100.0
        else:
            frac = None
        return n, frac

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

    def visitFugueCheckpointWeak(self, ctx: fp.FugueCheckpointWeakContext) -> Any:
        lazy = ctx.LAZY() is not None
        data = self.get_dict(ctx, "params")
        if "params" not in data:
            return lambda name, x: x.persist()
        else:
            return lambda name, x: x.weak_checkpoint(lazy=lazy, **data["params"])

    def visitFugueCheckpointStrong(self, ctx: fp.FugueCheckpointStrongContext) -> Any:
        lazy = ctx.LAZY() is not None
        data = self.get_dict(ctx, "partition", "single", "params")
        return lambda name, x: x.strong_checkpoint(
            lazy=lazy,
            partition=data.get("partition"),
            single="single" in data,
            **data.get("params", {}),
        )

    def visitFugueCheckpointDeterministic(
        self, ctx: fp.FugueCheckpointDeterministicContext
    ) -> Any:
        def _func(name: str, x: WorkflowDataFrame) -> WorkflowDataFrame:
            data = self.get_dict(ctx, "ns", "partition", "single", "params")

            x.deterministic_checkpoint(
                lazy=ctx.LAZY() is not None,
                partition=data.get("partition"),
                single="single" in data,
                namespace=data.get("ns"),
                **data.get("params", {}),
            )
            return x

        return _func

    def visitFugueYield(self, ctx: fp.FugueYieldContext) -> Any:
        def _func(name: str, x: WorkflowDataFrame) -> WorkflowDataFrame:
            yield_name = self.ctxToStr(ctx.name) if ctx.name is not None else name
            assert_or_throw(yield_name is not None, "yield name is not specified")
            if ctx.DATAFRAME() is None:
                x.yield_file_as(yield_name)
            else:
                x.yield_dataframe_as(yield_name)
            return x

        return _func

    def visitFugueCheckpointNamespace(self, ctx: fp.FugueCheckpointNamespaceContext):
        return str(eval(self.ctxToStr(ctx)))


class _Extensions(_VisitorBase):
    def __init__(
        self,
        sql: FugueSQL,
        hooks: FugueSQLHooks,
        workflow: FugueWorkflow,
        variables: Optional[
            Dict[
                str, Tuple[WorkflowDataFrame, WorkflowDataFrames, LazyWorkflowDataFrame]
            ]
        ] = None,
        last: Optional[WorkflowDataFrame] = None,
        global_vars: Optional[Dict[str, Any]] = None,
        local_vars: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(sql)
        self._workflow = workflow
        self._variables: Dict[
            str, Tuple[WorkflowDataFrame, WorkflowDataFrames, LazyWorkflowDataFrame]
        ] = {}
        if variables is not None:
            self._variables.update(variables)
        self._last: Optional[WorkflowDataFrame] = last
        self._hooks = hooks
        self._global_vars, self._local_vars = get_caller_global_local_vars(
            global_vars, local_vars
        )

    @property
    def workflow(self) -> FugueWorkflow:
        return self._workflow

    @property
    def hooks(self) -> FugueSQLHooks:
        return self._hooks

    @property
    def variables(
        self,
    ) -> Dict[str, Tuple[WorkflowDataFrame, WorkflowDataFrames, LazyWorkflowDataFrame]]:
        return self._variables

    @property
    def last(self) -> WorkflowDataFrame:
        if self._last is None:
            raise FugueSQLError("latest dataframe does not exist")
        return self._last

    @property
    def global_vars(self) -> Dict[str, Any]:
        return self._global_vars

    @property
    def local_vars(self) -> Dict[str, Any]:
        return self._local_vars

    def get_df(
        self, key: str, ctx: fp.FugueDataFrameMemberContext
    ) -> WorkflowDataFrame:
        assert_or_throw(
            key in self.variables,
            lambda: FugueSQLSyntaxError(f"{key} is not defined"),
        )
        if isinstance(self.variables[key], LazyWorkflowDataFrame):
            assert_or_throw(
                ctx is None,
                FugueSQLSyntaxError("can't specify index or key for dataframe"),
            )
            return self.variables[key].get_df()  # type: ignore
        if isinstance(self.variables[key], WorkflowDataFrame):
            assert_or_throw(
                ctx is None,
                FugueSQLSyntaxError("can't specify index or key for dataframe"),
            )
            return self.variables[key]  # type: ignore
        assert_or_throw(
            ctx is not None,
            FugueSQLSyntaxError("must specify index or key for dataframes"),
        )
        if ctx.index is not None:
            return self.variables[key][int(self.ctxToStr(ctx.index))]
        else:
            return self.variables[key][self.ctxToStr(ctx.key)]  # type: ignore

    def visitFugueDataFrameSource(
        self, ctx: fp.FugueDataFrameSourceContext
    ) -> WorkflowDataFrame:
        name = self.ctxToStr(ctx.fugueIdentifier(), delimit="")
        return self.get_df(name, ctx.fugueDataFrameMember())

    def visitFugueDataFrameNested(
        self, ctx: fp.FugueDataFrameNestedContext
    ) -> WorkflowDataFrame:
        sub = _Extensions(
            self.sql,
            self.hooks,
            workflow=self.workflow,
            variables=self.variables,
            last=self._last,
            global_vars=self.global_vars,
            local_vars=self.local_vars,
        )
        sub.visit(ctx.task)
        return sub.last

    def visitFugueDataFramePair(self, ctx: fp.FugueDataFramePairContext) -> Any:
        return self.to_kv(ctx)

    def visitFugueDataFramesList(
        self, ctx: fp.FugueDataFramesListContext
    ) -> WorkflowDataFrames:
        dfs = self.collectChildren(ctx, fp.FugueDataFrameContext)
        return WorkflowDataFrames(dfs)

    def visitFugueDataFramesDict(
        self, ctx: fp.FugueDataFramesDictContext
    ) -> WorkflowDataFrames:
        dfs = self.collectChildren(ctx, fp.FugueDataFramePairContext)
        return WorkflowDataFrames(dfs)

    def visitFugueTransformTask(
        self, ctx: fp.FugueTransformTaskContext
    ) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "partition", "dfs", "params", "callback")
        if "dfs" not in data:
            data["dfs"] = WorkflowDataFrames(self.last)
        p = data["params"]
        using = _to_transformer(
            p["using"],
            schema=p.get("schema"),
            global_vars=self.global_vars,
            local_vars=self.local_vars,
        )
        __modified_exception__ = self.to_runtime_error(ctx)  # noqa
        # TODO: ignore errors is not implemented
        return self.workflow.transform(
            data["dfs"],
            using=using,
            params=p.get("params"),
            pre_partition=data.get("partition"),
            callback=to_function(data["callback"], self.global_vars, self.local_vars)
            if "callback" in data
            else None,
        )

    def visitFugueOutputTransformTask(
        self, ctx: fp.FugueOutputTransformTaskContext
    ) -> None:
        data = self.get_dict(ctx, "partition", "dfs", "using", "params", "callback")
        if "dfs" not in data:
            data["dfs"] = WorkflowDataFrames(self.last)
        using = _to_output_transformer(
            data["using"],
            global_vars=self.global_vars,
            local_vars=self.local_vars,
        )
        __modified_exception__ = self.to_runtime_error(ctx)  # noqa
        # TODO: ignore errors is not implemented
        self.workflow.out_transform(
            data["dfs"],
            using=using,
            params=data.get("params"),
            pre_partition=data.get("partition"),
            callback=to_function(data["callback"], self.global_vars, self.local_vars)
            if "callback" in data
            else None,
        )

    def visitFugueProcessTask(
        self, ctx: fp.FugueProcessTaskContext
    ) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "partition", "dfs", "params")
        if "dfs" not in data:
            data["dfs"] = WorkflowDataFrames(self.last)
        p = data["params"]
        using = _to_processor(
            p["using"],
            schema=p.get("schema"),
            global_vars=self.global_vars,
            local_vars=self.local_vars,
        )
        __modified_exception__ = self.to_runtime_error(ctx)  # noqa
        return self.workflow.process(
            data["dfs"],
            using=using,
            params=p.get("params"),
            pre_partition=data.get("partition"),
        )

    def visitFugueCreateTask(self, ctx: fp.FugueCreateTaskContext) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "params")
        p = data["params"]
        using = _to_creator(
            p["using"],
            schema=p.get("schema"),
            global_vars=self.global_vars,
            local_vars=self.local_vars,
        )
        __modified_exception__ = self.to_runtime_error(ctx)  # noqa
        return self.workflow.create(using=using, params=p.get("params"))

    def visitFugueCreateDataTask(
        self, ctx: fp.FugueCreateDataTaskContext
    ) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "data", "schema")
        __modified_exception__ = self.to_runtime_error(ctx)  # noqa
        return self.workflow.df(
            data["data"], schema=data["schema"], data_determiner=to_uuid
        )

    def visitFugueZipTask(self, ctx: fp.FugueZipTaskContext) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "dfs", "how")
        partition_spec = PartitionSpec(**self.get_dict(ctx, "by", "presort"))
        __modified_exception__ = self.to_runtime_error(ctx)  # noqa
        # TODO: currently SQL does not support cache to file on ZIP
        return self.workflow.zip(
            data["dfs"], how=data.get("how", "inner"), partition=partition_spec
        )

    def visitFugueOutputTask(self, ctx: fp.FugueOutputTaskContext):
        data = self.get_dict(ctx, "dfs", "using", "params", "partition")
        if "dfs" not in data:
            data["dfs"] = WorkflowDataFrames(self.last)
        using = _to_outputter(
            data["using"],
            global_vars=self.global_vars,
            local_vars=self.local_vars,
        )
        __modified_exception__ = self.to_runtime_error(ctx)  # noqa
        self.workflow.output(
            data["dfs"],
            using=using,
            params=data.get("params"),
            pre_partition=data.get("partition"),
        )

    def visitFuguePrintTask(self, ctx: fp.FuguePrintTaskContext) -> None:
        data = self.get_dict(ctx, "dfs")
        if "dfs" not in data:
            data["dfs"] = WorkflowDataFrames(self.last)
        params: Dict[str, Any] = {}
        if ctx.rows is not None:
            params["rows"] = int(self.ctxToStr(ctx.rows))
        if ctx.count is not None:
            params["show_count"] = True
        if ctx.title is not None:
            params["title"] = eval(self.ctxToStr(ctx.title))
        __modified_exception__ = self.to_runtime_error(ctx)  # noqa
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

    def visitFugueSaveAndUseTask(self, ctx: fp.FugueSaveAndUseTaskContext):
        data = self.get_dict(
            ctx, "partition", "df", "m", "single", "fmt", "path", "params"
        )
        if "df" in data:
            df = data["df"]
        else:
            df = self.last
        return df.save_and_use(
            path=data["path"],
            fmt=data.get("fmt", ""),
            mode=data["m"],
            partition=data.get("partition"),
            single="single" in data,
            **data.get("params", {}),
        )

    def visitFugueRenameColumnsTask(self, ctx: fp.FugueRenameColumnsTaskContext):
        data = self.get_dict(ctx, "cols", "df")
        if "df" in data:
            df = data["df"]
        else:
            df = self.last
        return df.rename(data["cols"])

    def visitFugueAlterColumnsTask(self, ctx: fp.FugueAlterColumnsTaskContext):
        data = self.get_dict(ctx, "cols", "df")
        if "df" in data:
            df = data["df"]
        else:
            df = self.last
        return df.alter_columns(data["cols"])

    def visitFugueDropColumnsTask(self, ctx: fp.FugueDropColumnsTaskContext):
        data = self.get_dict(ctx, "cols", "df")
        if "df" in data:
            df = data["df"]
        else:
            df = self.last
        return df.drop(
            columns=data["cols"],
            if_exists=ctx.IF() is not None,
        )

    def visitFugueDropnaTask(self, ctx: fp.FugueDropnaTaskContext):
        data = self.get_dict(ctx, "cols", "df")
        if "df" in data:
            df = data["df"]
        else:
            df = self.last
        params: Dict[str, Any] = {}
        params["how"] = "any" if ctx.ANY() is not None else "all"
        if "cols" in data:
            params["subset"] = data["cols"]
        return df.dropna(**params)

    def visitFugueFillnaTask(self, ctx: fp.FugueFillnaTaskContext):
        data = self.get_dict(ctx, "params", "df")
        if "df" in data:
            df = data["df"]
        else:
            df = self.last
        return df.fillna(value=data["params"])

    def visitFugueSampleTask(self, ctx: fp.FugueSampleTaskContext):
        data = self.get_dict(ctx, "df")
        if "df" in data:
            df = data["df"]
        else:
            df = self.last
        params: Dict[str, Any] = {}
        params["replace"] = ctx.REPLACE() is not None
        if ctx.seed is not None:
            params["seed"] = int(self.ctxToStr(ctx.seed))
        n, frac = self.visit(ctx.method)
        if n is not None:
            params["n"] = n
        if frac is not None:
            params["frac"] = frac
        return df.sample(**params)

    def visitFugueTakeTask(self, ctx: fp.FugueTakeTaskContext):
        data = self.get_dict(ctx, "partition", "presort", "df")
        if "df" in data:
            df = data["df"]
        else:
            df = self.last
        params: Dict[str, Any] = {}
        params["n"] = int(self.ctxToStr(ctx.rows)) or 20  # default is 20
        params["na_position"] = "first" if ctx.FIRST() is not None else "last"
        if data.get("partition"):
            _partition_spec = PartitionSpec(data.get("partition"))
            return df.partition(
                by=_partition_spec.partition_by, presort=_partition_spec.presort
            ).take(**params)
        else:
            if data.get("presort"):
                params["presort"] = data.get("presort")
            return df.take(**params)

    def visitFugueLoadTask(self, ctx: fp.FugueLoadTaskContext) -> WorkflowDataFrame:
        data = self.get_dict(ctx, "fmt", "path", "params", "columns")
        __modified_exception__ = self.to_runtime_error(ctx)  # noqa
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
            __modified_exception__ = self.to_runtime_error(ctx)  # noqa
            df = self.workflow.select(*statements)
        self._process_assignable(df, ctx)

    def visitFugueModuleTask(self, ctx: fp.FugueModuleTaskContext) -> None:
        data = self.get_dict(ctx, "assign", "dfs", "using", "params")
        sub = _to_module(
            data["using"],
            global_vars=self.global_vars,
            local_vars=self.local_vars,
        )
        varname = data["assign"][0] if "assign" in data else None
        if varname is not None:
            assert_or_throw(
                sub.has_single_output or sub.has_multiple_output,
                FugueSQLSyntaxError("invalid assignment for module without output"),
            )
        if sub.has_input:
            dfs = data["dfs"] if "dfs" in data else WorkflowDataFrames(self.last)
        else:
            dfs = WorkflowDataFrames()
        p = data["params"] if "params" in data else {}
        if sub.has_dfs_input:
            result = sub(dfs, **p)
        elif len(dfs) == 0:
            result = sub(self.workflow, **p)
        elif len(dfs) == 1 or not dfs.has_key:
            result = sub(*list(dfs.values()), **p)
        else:
            result = sub(**dfs, **p)
        if sub.has_single_output or sub.has_multiple_output:
            self.variables[varname] = result
        if sub.has_single_output:
            self._last = result

    def visitFugueSqlEngine(
        self, ctx: fp.FugueSqlEngineContext
    ) -> Tuple[Any, Dict[str, Any]]:
        data = self.get_dict(ctx, "using", "params")
        try:
            engine: Any = to_type(
                data["using"],
                SQLEngine,
                global_vars=self.global_vars,
                local_vars=self.local_vars,
            )
        except TypeError:
            engine = str(data["using"])
        return engine, data.get("params", {})

    def visitQuery(self, ctx: fp.QueryContext) -> Iterable[Any]:
        def get_sql() -> str:
            return " ".join(
                [
                    "" if ctx.ctes() is None else self.ctxToStr(ctx.ctes()),
                    self.ctxToStr(ctx.queryTerm()),
                    self.ctxToStr(ctx.queryOrganization()),
                ]
            ).strip()

        if ctx.fugueSqlEngine() is not None:
            engine, engine_params = self.visitFugueSqlEngine(ctx.fugueSqlEngine())
            __modified_exception__ = self.to_runtime_error(ctx)  # noqa
            yield self.workflow.select(
                get_sql(), sql_engine=engine, sql_engine_params=engine_params
            )
        elif ctx.ctes() is None:
            yield from self._get_query_elements(ctx)
        else:
            __modified_exception__ = self.to_runtime_error(ctx)  # noqa
            yield self.workflow.select(get_sql())

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
            assert_or_throw(
                ctx.fugueDataFrameMember() is None,
                FugueSQLSyntaxError("can't specify index or key for dataframe"),
            )
            table: Any = self.hooks.on_select_source_not_found(
                self.workflow, table_name
            )
        else:
            table = self.get_df(table_name, ctx.fugueDataFrameMember())
        if isinstance(table, str):
            yield table
            yield from self._get_query_elements(ctx.sample())
            yield from self._get_query_elements(ctx.tableAlias())
        else:
            yield table
            yield from self._get_query_elements(ctx.sample())
            if ctx.tableAlias().strictIdentifier() is not None:
                yield from self._get_query_elements(ctx.tableAlias())
            elif validate_triad_var_name(table_name):
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
                global_vars=self.global_vars,
                local_vars=self.local_vars,
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
        data = self.get_dict(ctx, "assign", "checkpoint", "broadcast", "y")
        if "assign" in data:
            varname, _ = data["assign"]
        else:
            varname = None
        if "checkpoint" in data:
            data["checkpoint"](varname, df)
        if "broadcast" in data:
            df = df.broadcast()
        if "y" in data:
            data["y"](varname, df)
        if varname is not None:
            self.variables[varname] = df  # type: ignore
        self._last = df
