# Generated from fugue_sql/_antlr/fugue_sql.g4 by ANTLR 4.9
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .fugue_sqlParser import fugue_sqlParser
else:
    from fugue_sqlParser import fugue_sqlParser

# This class defines a complete generic visitor for a parse tree produced by fugue_sqlParser.

class fugue_sqlVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by fugue_sqlParser#fugueLanguage.
    def visitFugueLanguage(self, ctx:fugue_sqlParser.FugueLanguageContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSingleStatement.
    def visitFugueSingleStatement(self, ctx:fugue_sqlParser.FugueSingleStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSingleTask.
    def visitFugueSingleTask(self, ctx:fugue_sqlParser.FugueSingleTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueNestableTask.
    def visitFugueNestableTask(self, ctx:fugue_sqlParser.FugueNestableTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueNestableTaskCollectionNoSelect.
    def visitFugueNestableTaskCollectionNoSelect(self, ctx:fugue_sqlParser.FugueNestableTaskCollectionNoSelectContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueTransformTask.
    def visitFugueTransformTask(self, ctx:fugue_sqlParser.FugueTransformTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueProcessTask.
    def visitFugueProcessTask(self, ctx:fugue_sqlParser.FugueProcessTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSaveAndUseTask.
    def visitFugueSaveAndUseTask(self, ctx:fugue_sqlParser.FugueSaveAndUseTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueRenameColumnsTask.
    def visitFugueRenameColumnsTask(self, ctx:fugue_sqlParser.FugueRenameColumnsTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueAlterColumnsTask.
    def visitFugueAlterColumnsTask(self, ctx:fugue_sqlParser.FugueAlterColumnsTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueDropColumnsTask.
    def visitFugueDropColumnsTask(self, ctx:fugue_sqlParser.FugueDropColumnsTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueDropnaTask.
    def visitFugueDropnaTask(self, ctx:fugue_sqlParser.FugueDropnaTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueFillnaTask.
    def visitFugueFillnaTask(self, ctx:fugue_sqlParser.FugueFillnaTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSampleTask.
    def visitFugueSampleTask(self, ctx:fugue_sqlParser.FugueSampleTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueTakeTask.
    def visitFugueTakeTask(self, ctx:fugue_sqlParser.FugueTakeTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueZipTask.
    def visitFugueZipTask(self, ctx:fugue_sqlParser.FugueZipTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueCreateTask.
    def visitFugueCreateTask(self, ctx:fugue_sqlParser.FugueCreateTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueCreateDataTask.
    def visitFugueCreateDataTask(self, ctx:fugue_sqlParser.FugueCreateDataTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueLoadTask.
    def visitFugueLoadTask(self, ctx:fugue_sqlParser.FugueLoadTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueOutputTask.
    def visitFugueOutputTask(self, ctx:fugue_sqlParser.FugueOutputTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fuguePrintTask.
    def visitFuguePrintTask(self, ctx:fugue_sqlParser.FuguePrintTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSaveTask.
    def visitFugueSaveTask(self, ctx:fugue_sqlParser.FugueSaveTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueOutputTransformTask.
    def visitFugueOutputTransformTask(self, ctx:fugue_sqlParser.FugueOutputTransformTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueModuleTask.
    def visitFugueModuleTask(self, ctx:fugue_sqlParser.FugueModuleTaskContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSqlEngine.
    def visitFugueSqlEngine(self, ctx:fugue_sqlParser.FugueSqlEngineContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSingleFile.
    def visitFugueSingleFile(self, ctx:fugue_sqlParser.FugueSingleFileContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueLoadColumns.
    def visitFugueLoadColumns(self, ctx:fugue_sqlParser.FugueLoadColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSaveMode.
    def visitFugueSaveMode(self, ctx:fugue_sqlParser.FugueSaveModeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueFileFormat.
    def visitFugueFileFormat(self, ctx:fugue_sqlParser.FugueFileFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fuguePath.
    def visitFuguePath(self, ctx:fugue_sqlParser.FuguePathContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueCheckpointWeak.
    def visitFugueCheckpointWeak(self, ctx:fugue_sqlParser.FugueCheckpointWeakContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueCheckpointStrong.
    def visitFugueCheckpointStrong(self, ctx:fugue_sqlParser.FugueCheckpointStrongContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueCheckpointDeterministic.
    def visitFugueCheckpointDeterministic(self, ctx:fugue_sqlParser.FugueCheckpointDeterministicContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueCheckpointNamespace.
    def visitFugueCheckpointNamespace(self, ctx:fugue_sqlParser.FugueCheckpointNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueYield.
    def visitFugueYield(self, ctx:fugue_sqlParser.FugueYieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueBroadcast.
    def visitFugueBroadcast(self, ctx:fugue_sqlParser.FugueBroadcastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueDataFramesList.
    def visitFugueDataFramesList(self, ctx:fugue_sqlParser.FugueDataFramesListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueDataFramesDict.
    def visitFugueDataFramesDict(self, ctx:fugue_sqlParser.FugueDataFramesDictContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueDataFramePair.
    def visitFugueDataFramePair(self, ctx:fugue_sqlParser.FugueDataFramePairContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueDataFrameSource.
    def visitFugueDataFrameSource(self, ctx:fugue_sqlParser.FugueDataFrameSourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueDataFrameNested.
    def visitFugueDataFrameNested(self, ctx:fugue_sqlParser.FugueDataFrameNestedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueDataFrameMember.
    def visitFugueDataFrameMember(self, ctx:fugue_sqlParser.FugueDataFrameMemberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueAssignment.
    def visitFugueAssignment(self, ctx:fugue_sqlParser.FugueAssignmentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueAssignmentSign.
    def visitFugueAssignmentSign(self, ctx:fugue_sqlParser.FugueAssignmentSignContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSingleOutputExtensionCommonWild.
    def visitFugueSingleOutputExtensionCommonWild(self, ctx:fugue_sqlParser.FugueSingleOutputExtensionCommonWildContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSingleOutputExtensionCommon.
    def visitFugueSingleOutputExtensionCommon(self, ctx:fugue_sqlParser.FugueSingleOutputExtensionCommonContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueExtension.
    def visitFugueExtension(self, ctx:fugue_sqlParser.FugueExtensionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSampleMethod.
    def visitFugueSampleMethod(self, ctx:fugue_sqlParser.FugueSampleMethodContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueZipType.
    def visitFugueZipType(self, ctx:fugue_sqlParser.FugueZipTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fuguePrepartition.
    def visitFuguePrepartition(self, ctx:fugue_sqlParser.FuguePrepartitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fuguePartitionAlgo.
    def visitFuguePartitionAlgo(self, ctx:fugue_sqlParser.FuguePartitionAlgoContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fuguePartitionNum.
    def visitFuguePartitionNum(self, ctx:fugue_sqlParser.FuguePartitionNumContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fuguePartitionNumber.
    def visitFuguePartitionNumber(self, ctx:fugue_sqlParser.FuguePartitionNumberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueParamsPairs.
    def visitFugueParamsPairs(self, ctx:fugue_sqlParser.FugueParamsPairsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueParamsObj.
    def visitFugueParamsObj(self, ctx:fugue_sqlParser.FugueParamsObjContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueCols.
    def visitFugueCols(self, ctx:fugue_sqlParser.FugueColsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueColsSort.
    def visitFugueColsSort(self, ctx:fugue_sqlParser.FugueColsSortContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueColSort.
    def visitFugueColSort(self, ctx:fugue_sqlParser.FugueColSortContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueColumnIdentifier.
    def visitFugueColumnIdentifier(self, ctx:fugue_sqlParser.FugueColumnIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueRenameExpression.
    def visitFugueRenameExpression(self, ctx:fugue_sqlParser.FugueRenameExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueWildSchema.
    def visitFugueWildSchema(self, ctx:fugue_sqlParser.FugueWildSchemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueWildSchemaPair.
    def visitFugueWildSchemaPair(self, ctx:fugue_sqlParser.FugueWildSchemaPairContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSchema.
    def visitFugueSchema(self, ctx:fugue_sqlParser.FugueSchemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSchemaPair.
    def visitFugueSchemaPair(self, ctx:fugue_sqlParser.FugueSchemaPairContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSchemaKey.
    def visitFugueSchemaKey(self, ctx:fugue_sqlParser.FugueSchemaKeyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSchemaSimpleType.
    def visitFugueSchemaSimpleType(self, ctx:fugue_sqlParser.FugueSchemaSimpleTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSchemaListType.
    def visitFugueSchemaListType(self, ctx:fugue_sqlParser.FugueSchemaListTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueSchemaStructType.
    def visitFugueSchemaStructType(self, ctx:fugue_sqlParser.FugueSchemaStructTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueRenamePair.
    def visitFugueRenamePair(self, ctx:fugue_sqlParser.FugueRenamePairContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJson.
    def visitFugueJson(self, ctx:fugue_sqlParser.FugueJsonContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonObj.
    def visitFugueJsonObj(self, ctx:fugue_sqlParser.FugueJsonObjContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonPairs.
    def visitFugueJsonPairs(self, ctx:fugue_sqlParser.FugueJsonPairsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonPair.
    def visitFugueJsonPair(self, ctx:fugue_sqlParser.FugueJsonPairContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonKey.
    def visitFugueJsonKey(self, ctx:fugue_sqlParser.FugueJsonKeyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonArray.
    def visitFugueJsonArray(self, ctx:fugue_sqlParser.FugueJsonArrayContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonValue.
    def visitFugueJsonValue(self, ctx:fugue_sqlParser.FugueJsonValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonNumber.
    def visitFugueJsonNumber(self, ctx:fugue_sqlParser.FugueJsonNumberContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonString.
    def visitFugueJsonString(self, ctx:fugue_sqlParser.FugueJsonStringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonBool.
    def visitFugueJsonBool(self, ctx:fugue_sqlParser.FugueJsonBoolContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueJsonNull.
    def visitFugueJsonNull(self, ctx:fugue_sqlParser.FugueJsonNullContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueIdentifier.
    def visitFugueIdentifier(self, ctx:fugue_sqlParser.FugueIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#singleStatement.
    def visitSingleStatement(self, ctx:fugue_sqlParser.SingleStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#singleExpression.
    def visitSingleExpression(self, ctx:fugue_sqlParser.SingleExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#singleTableIdentifier.
    def visitSingleTableIdentifier(self, ctx:fugue_sqlParser.SingleTableIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#singleMultipartIdentifier.
    def visitSingleMultipartIdentifier(self, ctx:fugue_sqlParser.SingleMultipartIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#singleFunctionIdentifier.
    def visitSingleFunctionIdentifier(self, ctx:fugue_sqlParser.SingleFunctionIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#singleDataType.
    def visitSingleDataType(self, ctx:fugue_sqlParser.SingleDataTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#singleTableSchema.
    def visitSingleTableSchema(self, ctx:fugue_sqlParser.SingleTableSchemaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#statementDefault.
    def visitStatementDefault(self, ctx:fugue_sqlParser.StatementDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#dmlStatement.
    def visitDmlStatement(self, ctx:fugue_sqlParser.DmlStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#use.
    def visitUse(self, ctx:fugue_sqlParser.UseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createNamespace.
    def visitCreateNamespace(self, ctx:fugue_sqlParser.CreateNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#setNamespaceProperties.
    def visitSetNamespaceProperties(self, ctx:fugue_sqlParser.SetNamespacePropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#setNamespaceLocation.
    def visitSetNamespaceLocation(self, ctx:fugue_sqlParser.SetNamespaceLocationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#dropNamespace.
    def visitDropNamespace(self, ctx:fugue_sqlParser.DropNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showNamespaces.
    def visitShowNamespaces(self, ctx:fugue_sqlParser.ShowNamespacesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createTable.
    def visitCreateTable(self, ctx:fugue_sqlParser.CreateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createHiveTable.
    def visitCreateHiveTable(self, ctx:fugue_sqlParser.CreateHiveTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createTableLike.
    def visitCreateTableLike(self, ctx:fugue_sqlParser.CreateTableLikeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#replaceTable.
    def visitReplaceTable(self, ctx:fugue_sqlParser.ReplaceTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#analyze.
    def visitAnalyze(self, ctx:fugue_sqlParser.AnalyzeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#addTableColumns.
    def visitAddTableColumns(self, ctx:fugue_sqlParser.AddTableColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#renameTableColumn.
    def visitRenameTableColumn(self, ctx:fugue_sqlParser.RenameTableColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#dropTableColumns.
    def visitDropTableColumns(self, ctx:fugue_sqlParser.DropTableColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#renameTable.
    def visitRenameTable(self, ctx:fugue_sqlParser.RenameTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#setTableProperties.
    def visitSetTableProperties(self, ctx:fugue_sqlParser.SetTablePropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#unsetTableProperties.
    def visitUnsetTableProperties(self, ctx:fugue_sqlParser.UnsetTablePropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#alterTableAlterColumn.
    def visitAlterTableAlterColumn(self, ctx:fugue_sqlParser.AlterTableAlterColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#hiveChangeColumn.
    def visitHiveChangeColumn(self, ctx:fugue_sqlParser.HiveChangeColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#hiveReplaceColumns.
    def visitHiveReplaceColumns(self, ctx:fugue_sqlParser.HiveReplaceColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#setTableSerDe.
    def visitSetTableSerDe(self, ctx:fugue_sqlParser.SetTableSerDeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#addTablePartition.
    def visitAddTablePartition(self, ctx:fugue_sqlParser.AddTablePartitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#renameTablePartition.
    def visitRenameTablePartition(self, ctx:fugue_sqlParser.RenameTablePartitionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#dropTablePartitions.
    def visitDropTablePartitions(self, ctx:fugue_sqlParser.DropTablePartitionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#setTableLocation.
    def visitSetTableLocation(self, ctx:fugue_sqlParser.SetTableLocationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#recoverPartitions.
    def visitRecoverPartitions(self, ctx:fugue_sqlParser.RecoverPartitionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#dropTable.
    def visitDropTable(self, ctx:fugue_sqlParser.DropTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#dropView.
    def visitDropView(self, ctx:fugue_sqlParser.DropViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createView.
    def visitCreateView(self, ctx:fugue_sqlParser.CreateViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createTempViewUsing.
    def visitCreateTempViewUsing(self, ctx:fugue_sqlParser.CreateTempViewUsingContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#alterViewQuery.
    def visitAlterViewQuery(self, ctx:fugue_sqlParser.AlterViewQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createFunction.
    def visitCreateFunction(self, ctx:fugue_sqlParser.CreateFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#dropFunction.
    def visitDropFunction(self, ctx:fugue_sqlParser.DropFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#explain.
    def visitExplain(self, ctx:fugue_sqlParser.ExplainContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showTables.
    def visitShowTables(self, ctx:fugue_sqlParser.ShowTablesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showTable.
    def visitShowTable(self, ctx:fugue_sqlParser.ShowTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showTblProperties.
    def visitShowTblProperties(self, ctx:fugue_sqlParser.ShowTblPropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showColumns.
    def visitShowColumns(self, ctx:fugue_sqlParser.ShowColumnsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showViews.
    def visitShowViews(self, ctx:fugue_sqlParser.ShowViewsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showPartitions.
    def visitShowPartitions(self, ctx:fugue_sqlParser.ShowPartitionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showFunctions.
    def visitShowFunctions(self, ctx:fugue_sqlParser.ShowFunctionsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showCreateTable.
    def visitShowCreateTable(self, ctx:fugue_sqlParser.ShowCreateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#showCurrentNamespace.
    def visitShowCurrentNamespace(self, ctx:fugue_sqlParser.ShowCurrentNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#describeFunction.
    def visitDescribeFunction(self, ctx:fugue_sqlParser.DescribeFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#describeNamespace.
    def visitDescribeNamespace(self, ctx:fugue_sqlParser.DescribeNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#describeRelation.
    def visitDescribeRelation(self, ctx:fugue_sqlParser.DescribeRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#describeQuery.
    def visitDescribeQuery(self, ctx:fugue_sqlParser.DescribeQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#commentNamespace.
    def visitCommentNamespace(self, ctx:fugue_sqlParser.CommentNamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#commentTable.
    def visitCommentTable(self, ctx:fugue_sqlParser.CommentTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#refreshTable.
    def visitRefreshTable(self, ctx:fugue_sqlParser.RefreshTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#refreshResource.
    def visitRefreshResource(self, ctx:fugue_sqlParser.RefreshResourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#cacheTable.
    def visitCacheTable(self, ctx:fugue_sqlParser.CacheTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#uncacheTable.
    def visitUncacheTable(self, ctx:fugue_sqlParser.UncacheTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#clearCache.
    def visitClearCache(self, ctx:fugue_sqlParser.ClearCacheContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#loadData.
    def visitLoadData(self, ctx:fugue_sqlParser.LoadDataContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#truncateTable.
    def visitTruncateTable(self, ctx:fugue_sqlParser.TruncateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#repairTable.
    def visitRepairTable(self, ctx:fugue_sqlParser.RepairTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#manageResource.
    def visitManageResource(self, ctx:fugue_sqlParser.ManageResourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#failNativeCommand.
    def visitFailNativeCommand(self, ctx:fugue_sqlParser.FailNativeCommandContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#setConfiguration.
    def visitSetConfiguration(self, ctx:fugue_sqlParser.SetConfigurationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#resetConfiguration.
    def visitResetConfiguration(self, ctx:fugue_sqlParser.ResetConfigurationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#unsupportedHiveNativeCommands.
    def visitUnsupportedHiveNativeCommands(self, ctx:fugue_sqlParser.UnsupportedHiveNativeCommandsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createTableHeader.
    def visitCreateTableHeader(self, ctx:fugue_sqlParser.CreateTableHeaderContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#replaceTableHeader.
    def visitReplaceTableHeader(self, ctx:fugue_sqlParser.ReplaceTableHeaderContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#bucketSpec.
    def visitBucketSpec(self, ctx:fugue_sqlParser.BucketSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#skewSpec.
    def visitSkewSpec(self, ctx:fugue_sqlParser.SkewSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#locationSpec.
    def visitLocationSpec(self, ctx:fugue_sqlParser.LocationSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#commentSpec.
    def visitCommentSpec(self, ctx:fugue_sqlParser.CommentSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#query.
    def visitQuery(self, ctx:fugue_sqlParser.QueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#insertOverwriteTable.
    def visitInsertOverwriteTable(self, ctx:fugue_sqlParser.InsertOverwriteTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#insertIntoTable.
    def visitInsertIntoTable(self, ctx:fugue_sqlParser.InsertIntoTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#insertOverwriteHiveDir.
    def visitInsertOverwriteHiveDir(self, ctx:fugue_sqlParser.InsertOverwriteHiveDirContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#insertOverwriteDir.
    def visitInsertOverwriteDir(self, ctx:fugue_sqlParser.InsertOverwriteDirContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#partitionSpecLocation.
    def visitPartitionSpecLocation(self, ctx:fugue_sqlParser.PartitionSpecLocationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#partitionSpec.
    def visitPartitionSpec(self, ctx:fugue_sqlParser.PartitionSpecContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#partitionVal.
    def visitPartitionVal(self, ctx:fugue_sqlParser.PartitionValContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#namespace.
    def visitNamespace(self, ctx:fugue_sqlParser.NamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#describeFuncName.
    def visitDescribeFuncName(self, ctx:fugue_sqlParser.DescribeFuncNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#describeColName.
    def visitDescribeColName(self, ctx:fugue_sqlParser.DescribeColNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#ctes.
    def visitCtes(self, ctx:fugue_sqlParser.CtesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#namedQuery.
    def visitNamedQuery(self, ctx:fugue_sqlParser.NamedQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tableProvider.
    def visitTableProvider(self, ctx:fugue_sqlParser.TableProviderContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createTableClauses.
    def visitCreateTableClauses(self, ctx:fugue_sqlParser.CreateTableClausesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tablePropertyList.
    def visitTablePropertyList(self, ctx:fugue_sqlParser.TablePropertyListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tableProperty.
    def visitTableProperty(self, ctx:fugue_sqlParser.TablePropertyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tablePropertyKey.
    def visitTablePropertyKey(self, ctx:fugue_sqlParser.TablePropertyKeyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tablePropertyValue.
    def visitTablePropertyValue(self, ctx:fugue_sqlParser.TablePropertyValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#constantList.
    def visitConstantList(self, ctx:fugue_sqlParser.ConstantListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#nestedConstantList.
    def visitNestedConstantList(self, ctx:fugue_sqlParser.NestedConstantListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#createFileFormat.
    def visitCreateFileFormat(self, ctx:fugue_sqlParser.CreateFileFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tableFileFormat.
    def visitTableFileFormat(self, ctx:fugue_sqlParser.TableFileFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#genericFileFormat.
    def visitGenericFileFormat(self, ctx:fugue_sqlParser.GenericFileFormatContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#storageHandler.
    def visitStorageHandler(self, ctx:fugue_sqlParser.StorageHandlerContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#resource.
    def visitResource(self, ctx:fugue_sqlParser.ResourceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#singleInsertQuery.
    def visitSingleInsertQuery(self, ctx:fugue_sqlParser.SingleInsertQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#multiInsertQuery.
    def visitMultiInsertQuery(self, ctx:fugue_sqlParser.MultiInsertQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#deleteFromTable.
    def visitDeleteFromTable(self, ctx:fugue_sqlParser.DeleteFromTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#updateTable.
    def visitUpdateTable(self, ctx:fugue_sqlParser.UpdateTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#mergeIntoTable.
    def visitMergeIntoTable(self, ctx:fugue_sqlParser.MergeIntoTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#queryOrganization.
    def visitQueryOrganization(self, ctx:fugue_sqlParser.QueryOrganizationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#multiInsertQueryBody.
    def visitMultiInsertQueryBody(self, ctx:fugue_sqlParser.MultiInsertQueryBodyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#queryTermDefault.
    def visitQueryTermDefault(self, ctx:fugue_sqlParser.QueryTermDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fugueTerm.
    def visitFugueTerm(self, ctx:fugue_sqlParser.FugueTermContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#setOperation.
    def visitSetOperation(self, ctx:fugue_sqlParser.SetOperationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#queryPrimaryDefault.
    def visitQueryPrimaryDefault(self, ctx:fugue_sqlParser.QueryPrimaryDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fromStmt.
    def visitFromStmt(self, ctx:fugue_sqlParser.FromStmtContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#table.
    def visitTable(self, ctx:fugue_sqlParser.TableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#inlineTableDefault1.
    def visitInlineTableDefault1(self, ctx:fugue_sqlParser.InlineTableDefault1Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#sortItem.
    def visitSortItem(self, ctx:fugue_sqlParser.SortItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fromStatement.
    def visitFromStatement(self, ctx:fugue_sqlParser.FromStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fromStatementBody.
    def visitFromStatementBody(self, ctx:fugue_sqlParser.FromStatementBodyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#transformQuerySpecification.
    def visitTransformQuerySpecification(self, ctx:fugue_sqlParser.TransformQuerySpecificationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#regularQuerySpecification.
    def visitRegularQuerySpecification(self, ctx:fugue_sqlParser.RegularQuerySpecificationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#optionalFromClause.
    def visitOptionalFromClause(self, ctx:fugue_sqlParser.OptionalFromClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#transformClause.
    def visitTransformClause(self, ctx:fugue_sqlParser.TransformClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#selectClause.
    def visitSelectClause(self, ctx:fugue_sqlParser.SelectClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#setClause.
    def visitSetClause(self, ctx:fugue_sqlParser.SetClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#matchedClause.
    def visitMatchedClause(self, ctx:fugue_sqlParser.MatchedClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#notMatchedClause.
    def visitNotMatchedClause(self, ctx:fugue_sqlParser.NotMatchedClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#matchedAction.
    def visitMatchedAction(self, ctx:fugue_sqlParser.MatchedActionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#notMatchedAction.
    def visitNotMatchedAction(self, ctx:fugue_sqlParser.NotMatchedActionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#assignmentList.
    def visitAssignmentList(self, ctx:fugue_sqlParser.AssignmentListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#assignment.
    def visitAssignment(self, ctx:fugue_sqlParser.AssignmentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#whereClause.
    def visitWhereClause(self, ctx:fugue_sqlParser.WhereClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#havingClause.
    def visitHavingClause(self, ctx:fugue_sqlParser.HavingClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#hint.
    def visitHint(self, ctx:fugue_sqlParser.HintContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#hintStatement.
    def visitHintStatement(self, ctx:fugue_sqlParser.HintStatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#fromClause.
    def visitFromClause(self, ctx:fugue_sqlParser.FromClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#aggregationClause.
    def visitAggregationClause(self, ctx:fugue_sqlParser.AggregationClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#groupingSet.
    def visitGroupingSet(self, ctx:fugue_sqlParser.GroupingSetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#pivotClause.
    def visitPivotClause(self, ctx:fugue_sqlParser.PivotClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#pivotColumn.
    def visitPivotColumn(self, ctx:fugue_sqlParser.PivotColumnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#pivotValue.
    def visitPivotValue(self, ctx:fugue_sqlParser.PivotValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#lateralView.
    def visitLateralView(self, ctx:fugue_sqlParser.LateralViewContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#setQuantifier.
    def visitSetQuantifier(self, ctx:fugue_sqlParser.SetQuantifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#relation.
    def visitRelation(self, ctx:fugue_sqlParser.RelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#joinRelation.
    def visitJoinRelation(self, ctx:fugue_sqlParser.JoinRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#joinType.
    def visitJoinType(self, ctx:fugue_sqlParser.JoinTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#joinCriteria.
    def visitJoinCriteria(self, ctx:fugue_sqlParser.JoinCriteriaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#sample.
    def visitSample(self, ctx:fugue_sqlParser.SampleContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#sampleByPercentile.
    def visitSampleByPercentile(self, ctx:fugue_sqlParser.SampleByPercentileContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#sampleByRows.
    def visitSampleByRows(self, ctx:fugue_sqlParser.SampleByRowsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#sampleByBucket.
    def visitSampleByBucket(self, ctx:fugue_sqlParser.SampleByBucketContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#sampleByBytes.
    def visitSampleByBytes(self, ctx:fugue_sqlParser.SampleByBytesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#identifierList.
    def visitIdentifierList(self, ctx:fugue_sqlParser.IdentifierListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#identifierSeq.
    def visitIdentifierSeq(self, ctx:fugue_sqlParser.IdentifierSeqContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#orderedIdentifierList.
    def visitOrderedIdentifierList(self, ctx:fugue_sqlParser.OrderedIdentifierListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#orderedIdentifier.
    def visitOrderedIdentifier(self, ctx:fugue_sqlParser.OrderedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#identifierCommentList.
    def visitIdentifierCommentList(self, ctx:fugue_sqlParser.IdentifierCommentListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#identifierComment.
    def visitIdentifierComment(self, ctx:fugue_sqlParser.IdentifierCommentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tableName.
    def visitTableName(self, ctx:fugue_sqlParser.TableNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#aliasedQuery.
    def visitAliasedQuery(self, ctx:fugue_sqlParser.AliasedQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#aliasedRelation.
    def visitAliasedRelation(self, ctx:fugue_sqlParser.AliasedRelationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#inlineTableDefault2.
    def visitInlineTableDefault2(self, ctx:fugue_sqlParser.InlineTableDefault2Context):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tableValuedFunction.
    def visitTableValuedFunction(self, ctx:fugue_sqlParser.TableValuedFunctionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#inlineTable.
    def visitInlineTable(self, ctx:fugue_sqlParser.InlineTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#functionTable.
    def visitFunctionTable(self, ctx:fugue_sqlParser.FunctionTableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tableAlias.
    def visitTableAlias(self, ctx:fugue_sqlParser.TableAliasContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#rowFormatSerde.
    def visitRowFormatSerde(self, ctx:fugue_sqlParser.RowFormatSerdeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#rowFormatDelimited.
    def visitRowFormatDelimited(self, ctx:fugue_sqlParser.RowFormatDelimitedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#multipartIdentifierList.
    def visitMultipartIdentifierList(self, ctx:fugue_sqlParser.MultipartIdentifierListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#multipartIdentifier.
    def visitMultipartIdentifier(self, ctx:fugue_sqlParser.MultipartIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tableIdentifier.
    def visitTableIdentifier(self, ctx:fugue_sqlParser.TableIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#functionIdentifier.
    def visitFunctionIdentifier(self, ctx:fugue_sqlParser.FunctionIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#namedExpression.
    def visitNamedExpression(self, ctx:fugue_sqlParser.NamedExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#namedExpressionSeq.
    def visitNamedExpressionSeq(self, ctx:fugue_sqlParser.NamedExpressionSeqContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#transformList.
    def visitTransformList(self, ctx:fugue_sqlParser.TransformListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#identityTransform.
    def visitIdentityTransform(self, ctx:fugue_sqlParser.IdentityTransformContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#applyTransform.
    def visitApplyTransform(self, ctx:fugue_sqlParser.ApplyTransformContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#transformArgument.
    def visitTransformArgument(self, ctx:fugue_sqlParser.TransformArgumentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#expression.
    def visitExpression(self, ctx:fugue_sqlParser.ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#logicalNot.
    def visitLogicalNot(self, ctx:fugue_sqlParser.LogicalNotContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#predicated.
    def visitPredicated(self, ctx:fugue_sqlParser.PredicatedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#exists.
    def visitExists(self, ctx:fugue_sqlParser.ExistsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#logicalBinary.
    def visitLogicalBinary(self, ctx:fugue_sqlParser.LogicalBinaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#predicate.
    def visitPredicate(self, ctx:fugue_sqlParser.PredicateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#valueExpressionDefault.
    def visitValueExpressionDefault(self, ctx:fugue_sqlParser.ValueExpressionDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#comparison.
    def visitComparison(self, ctx:fugue_sqlParser.ComparisonContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#arithmeticBinary.
    def visitArithmeticBinary(self, ctx:fugue_sqlParser.ArithmeticBinaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#arithmeticUnary.
    def visitArithmeticUnary(self, ctx:fugue_sqlParser.ArithmeticUnaryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#struct.
    def visitStruct(self, ctx:fugue_sqlParser.StructContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#dereference.
    def visitDereference(self, ctx:fugue_sqlParser.DereferenceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#simpleCase.
    def visitSimpleCase(self, ctx:fugue_sqlParser.SimpleCaseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#columnReference.
    def visitColumnReference(self, ctx:fugue_sqlParser.ColumnReferenceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#rowConstructor.
    def visitRowConstructor(self, ctx:fugue_sqlParser.RowConstructorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#last.
    def visitLast(self, ctx:fugue_sqlParser.LastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#star.
    def visitStar(self, ctx:fugue_sqlParser.StarContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#overlay.
    def visitOverlay(self, ctx:fugue_sqlParser.OverlayContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#subscript.
    def visitSubscript(self, ctx:fugue_sqlParser.SubscriptContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#subqueryExpression.
    def visitSubqueryExpression(self, ctx:fugue_sqlParser.SubqueryExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#substring.
    def visitSubstring(self, ctx:fugue_sqlParser.SubstringContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#currentDatetime.
    def visitCurrentDatetime(self, ctx:fugue_sqlParser.CurrentDatetimeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#cast.
    def visitCast(self, ctx:fugue_sqlParser.CastContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#constantDefault.
    def visitConstantDefault(self, ctx:fugue_sqlParser.ConstantDefaultContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#lambda.
    def visitLambda(self, ctx:fugue_sqlParser.LambdaContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#parenthesizedExpression.
    def visitParenthesizedExpression(self, ctx:fugue_sqlParser.ParenthesizedExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#extract.
    def visitExtract(self, ctx:fugue_sqlParser.ExtractContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#trim.
    def visitTrim(self, ctx:fugue_sqlParser.TrimContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#functionCall.
    def visitFunctionCall(self, ctx:fugue_sqlParser.FunctionCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#searchedCase.
    def visitSearchedCase(self, ctx:fugue_sqlParser.SearchedCaseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#position.
    def visitPosition(self, ctx:fugue_sqlParser.PositionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#first.
    def visitFirst(self, ctx:fugue_sqlParser.FirstContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#nullLiteral.
    def visitNullLiteral(self, ctx:fugue_sqlParser.NullLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#intervalLiteral.
    def visitIntervalLiteral(self, ctx:fugue_sqlParser.IntervalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#typeConstructor.
    def visitTypeConstructor(self, ctx:fugue_sqlParser.TypeConstructorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#numericLiteral.
    def visitNumericLiteral(self, ctx:fugue_sqlParser.NumericLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#booleanLiteral.
    def visitBooleanLiteral(self, ctx:fugue_sqlParser.BooleanLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#stringLiteral.
    def visitStringLiteral(self, ctx:fugue_sqlParser.StringLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#comparisonOperator.
    def visitComparisonOperator(self, ctx:fugue_sqlParser.ComparisonOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#comparisonEqualOperator.
    def visitComparisonEqualOperator(self, ctx:fugue_sqlParser.ComparisonEqualOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#arithmeticOperator.
    def visitArithmeticOperator(self, ctx:fugue_sqlParser.ArithmeticOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#predicateOperator.
    def visitPredicateOperator(self, ctx:fugue_sqlParser.PredicateOperatorContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#booleanValue.
    def visitBooleanValue(self, ctx:fugue_sqlParser.BooleanValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#interval.
    def visitInterval(self, ctx:fugue_sqlParser.IntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#errorCapturingMultiUnitsInterval.
    def visitErrorCapturingMultiUnitsInterval(self, ctx:fugue_sqlParser.ErrorCapturingMultiUnitsIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#multiUnitsInterval.
    def visitMultiUnitsInterval(self, ctx:fugue_sqlParser.MultiUnitsIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#errorCapturingUnitToUnitInterval.
    def visitErrorCapturingUnitToUnitInterval(self, ctx:fugue_sqlParser.ErrorCapturingUnitToUnitIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#unitToUnitInterval.
    def visitUnitToUnitInterval(self, ctx:fugue_sqlParser.UnitToUnitIntervalContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#intervalValue.
    def visitIntervalValue(self, ctx:fugue_sqlParser.IntervalValueContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#intervalUnit.
    def visitIntervalUnit(self, ctx:fugue_sqlParser.IntervalUnitContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#colPosition.
    def visitColPosition(self, ctx:fugue_sqlParser.ColPositionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#complexDataType.
    def visitComplexDataType(self, ctx:fugue_sqlParser.ComplexDataTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#primitiveDataType.
    def visitPrimitiveDataType(self, ctx:fugue_sqlParser.PrimitiveDataTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#qualifiedColTypeWithPositionList.
    def visitQualifiedColTypeWithPositionList(self, ctx:fugue_sqlParser.QualifiedColTypeWithPositionListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#qualifiedColTypeWithPosition.
    def visitQualifiedColTypeWithPosition(self, ctx:fugue_sqlParser.QualifiedColTypeWithPositionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#colTypeList.
    def visitColTypeList(self, ctx:fugue_sqlParser.ColTypeListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#colType.
    def visitColType(self, ctx:fugue_sqlParser.ColTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#complexColTypeList.
    def visitComplexColTypeList(self, ctx:fugue_sqlParser.ComplexColTypeListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#complexColType.
    def visitComplexColType(self, ctx:fugue_sqlParser.ComplexColTypeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#whenClause.
    def visitWhenClause(self, ctx:fugue_sqlParser.WhenClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#windowClause.
    def visitWindowClause(self, ctx:fugue_sqlParser.WindowClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#namedWindow.
    def visitNamedWindow(self, ctx:fugue_sqlParser.NamedWindowContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#windowRef.
    def visitWindowRef(self, ctx:fugue_sqlParser.WindowRefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#windowDef.
    def visitWindowDef(self, ctx:fugue_sqlParser.WindowDefContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#windowFrame.
    def visitWindowFrame(self, ctx:fugue_sqlParser.WindowFrameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#frameBound.
    def visitFrameBound(self, ctx:fugue_sqlParser.FrameBoundContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#qualifiedNameList.
    def visitQualifiedNameList(self, ctx:fugue_sqlParser.QualifiedNameListContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#functionName.
    def visitFunctionName(self, ctx:fugue_sqlParser.FunctionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#qualifiedName.
    def visitQualifiedName(self, ctx:fugue_sqlParser.QualifiedNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#errorCapturingIdentifier.
    def visitErrorCapturingIdentifier(self, ctx:fugue_sqlParser.ErrorCapturingIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#errorIdent.
    def visitErrorIdent(self, ctx:fugue_sqlParser.ErrorIdentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#realIdent.
    def visitRealIdent(self, ctx:fugue_sqlParser.RealIdentContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#identifier.
    def visitIdentifier(self, ctx:fugue_sqlParser.IdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#unquotedIdentifier.
    def visitUnquotedIdentifier(self, ctx:fugue_sqlParser.UnquotedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#quotedIdentifierAlternative.
    def visitQuotedIdentifierAlternative(self, ctx:fugue_sqlParser.QuotedIdentifierAlternativeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#quotedIdentifier.
    def visitQuotedIdentifier(self, ctx:fugue_sqlParser.QuotedIdentifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#exponentLiteral.
    def visitExponentLiteral(self, ctx:fugue_sqlParser.ExponentLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#decimalLiteral.
    def visitDecimalLiteral(self, ctx:fugue_sqlParser.DecimalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#legacyDecimalLiteral.
    def visitLegacyDecimalLiteral(self, ctx:fugue_sqlParser.LegacyDecimalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#integerLiteral.
    def visitIntegerLiteral(self, ctx:fugue_sqlParser.IntegerLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#bigIntLiteral.
    def visitBigIntLiteral(self, ctx:fugue_sqlParser.BigIntLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#smallIntLiteral.
    def visitSmallIntLiteral(self, ctx:fugue_sqlParser.SmallIntLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#tinyIntLiteral.
    def visitTinyIntLiteral(self, ctx:fugue_sqlParser.TinyIntLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#doubleLiteral.
    def visitDoubleLiteral(self, ctx:fugue_sqlParser.DoubleLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#bigDecimalLiteral.
    def visitBigDecimalLiteral(self, ctx:fugue_sqlParser.BigDecimalLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#alterColumnAction.
    def visitAlterColumnAction(self, ctx:fugue_sqlParser.AlterColumnActionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#ansiNonReserved.
    def visitAnsiNonReserved(self, ctx:fugue_sqlParser.AnsiNonReservedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#strictNonReserved.
    def visitStrictNonReserved(self, ctx:fugue_sqlParser.StrictNonReservedContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by fugue_sqlParser#nonReserved.
    def visitNonReserved(self, ctx:fugue_sqlParser.NonReservedContext):
        return self.visitChildren(ctx)



del fugue_sqlParser