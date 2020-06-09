# Generated from fugue_sql/antlr/fugue_sql.g4 by ANTLR 4.8
from antlr4 import *
if __name__ is not None and "." in __name__:
    from .fugue_sqlParser import fugue_sqlParser
else:
    from fugue_sqlParser import fugue_sqlParser

# This class defines a complete listener for a parse tree produced by fugue_sqlParser.
class fugue_sqlListener(ParseTreeListener):

    # Enter a parse tree produced by fugue_sqlParser#fugueLanguage.
    def enterFugueLanguage(self, ctx:fugue_sqlParser.FugueLanguageContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueLanguage.
    def exitFugueLanguage(self, ctx:fugue_sqlParser.FugueLanguageContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSingleStatement.
    def enterFugueSingleStatement(self, ctx:fugue_sqlParser.FugueSingleStatementContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSingleStatement.
    def exitFugueSingleStatement(self, ctx:fugue_sqlParser.FugueSingleStatementContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSingleTask.
    def enterFugueSingleTask(self, ctx:fugue_sqlParser.FugueSingleTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSingleTask.
    def exitFugueSingleTask(self, ctx:fugue_sqlParser.FugueSingleTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueNestableTask.
    def enterFugueNestableTask(self, ctx:fugue_sqlParser.FugueNestableTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueNestableTask.
    def exitFugueNestableTask(self, ctx:fugue_sqlParser.FugueNestableTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueNestableTaskNoSelect.
    def enterFugueNestableTaskNoSelect(self, ctx:fugue_sqlParser.FugueNestableTaskNoSelectContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueNestableTaskNoSelect.
    def exitFugueNestableTaskNoSelect(self, ctx:fugue_sqlParser.FugueNestableTaskNoSelectContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueNestableTaskCollectionNoSelect.
    def enterFugueNestableTaskCollectionNoSelect(self, ctx:fugue_sqlParser.FugueNestableTaskCollectionNoSelectContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueNestableTaskCollectionNoSelect.
    def exitFugueNestableTaskCollectionNoSelect(self, ctx:fugue_sqlParser.FugueNestableTaskCollectionNoSelectContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSelectTask.
    def enterFugueSelectTask(self, ctx:fugue_sqlParser.FugueSelectTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSelectTask.
    def exitFugueSelectTask(self, ctx:fugue_sqlParser.FugueSelectTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueTransformTask.
    def enterFugueTransformTask(self, ctx:fugue_sqlParser.FugueTransformTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueTransformTask.
    def exitFugueTransformTask(self, ctx:fugue_sqlParser.FugueTransformTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueProcessTask.
    def enterFugueProcessTask(self, ctx:fugue_sqlParser.FugueProcessTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueProcessTask.
    def exitFugueProcessTask(self, ctx:fugue_sqlParser.FugueProcessTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueZipTask.
    def enterFugueZipTask(self, ctx:fugue_sqlParser.FugueZipTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueZipTask.
    def exitFugueZipTask(self, ctx:fugue_sqlParser.FugueZipTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueCreateTask.
    def enterFugueCreateTask(self, ctx:fugue_sqlParser.FugueCreateTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueCreateTask.
    def exitFugueCreateTask(self, ctx:fugue_sqlParser.FugueCreateTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueCreateDataTask.
    def enterFugueCreateDataTask(self, ctx:fugue_sqlParser.FugueCreateDataTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueCreateDataTask.
    def exitFugueCreateDataTask(self, ctx:fugue_sqlParser.FugueCreateDataTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueOutputTask.
    def enterFugueOutputTask(self, ctx:fugue_sqlParser.FugueOutputTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueOutputTask.
    def exitFugueOutputTask(self, ctx:fugue_sqlParser.FugueOutputTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fuguePrintTask.
    def enterFuguePrintTask(self, ctx:fugue_sqlParser.FuguePrintTaskContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fuguePrintTask.
    def exitFuguePrintTask(self, ctx:fugue_sqlParser.FuguePrintTaskContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fuguePersist.
    def enterFuguePersist(self, ctx:fugue_sqlParser.FuguePersistContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fuguePersist.
    def exitFuguePersist(self, ctx:fugue_sqlParser.FuguePersistContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueBroadcast.
    def enterFugueBroadcast(self, ctx:fugue_sqlParser.FugueBroadcastContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueBroadcast.
    def exitFugueBroadcast(self, ctx:fugue_sqlParser.FugueBroadcastContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueDataFramesList.
    def enterFugueDataFramesList(self, ctx:fugue_sqlParser.FugueDataFramesListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueDataFramesList.
    def exitFugueDataFramesList(self, ctx:fugue_sqlParser.FugueDataFramesListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueDataFramesDict.
    def enterFugueDataFramesDict(self, ctx:fugue_sqlParser.FugueDataFramesDictContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueDataFramesDict.
    def exitFugueDataFramesDict(self, ctx:fugue_sqlParser.FugueDataFramesDictContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueDataFramePair.
    def enterFugueDataFramePair(self, ctx:fugue_sqlParser.FugueDataFramePairContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueDataFramePair.
    def exitFugueDataFramePair(self, ctx:fugue_sqlParser.FugueDataFramePairContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueDataFrameSource.
    def enterFugueDataFrameSource(self, ctx:fugue_sqlParser.FugueDataFrameSourceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueDataFrameSource.
    def exitFugueDataFrameSource(self, ctx:fugue_sqlParser.FugueDataFrameSourceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueDataFrameNested.
    def enterFugueDataFrameNested(self, ctx:fugue_sqlParser.FugueDataFrameNestedContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueDataFrameNested.
    def exitFugueDataFrameNested(self, ctx:fugue_sqlParser.FugueDataFrameNestedContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueAssignment.
    def enterFugueAssignment(self, ctx:fugue_sqlParser.FugueAssignmentContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueAssignment.
    def exitFugueAssignment(self, ctx:fugue_sqlParser.FugueAssignmentContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueAssignmentSign.
    def enterFugueAssignmentSign(self, ctx:fugue_sqlParser.FugueAssignmentSignContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueAssignmentSign.
    def exitFugueAssignmentSign(self, ctx:fugue_sqlParser.FugueAssignmentSignContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSingleOutputExtensionCommonWild.
    def enterFugueSingleOutputExtensionCommonWild(self, ctx:fugue_sqlParser.FugueSingleOutputExtensionCommonWildContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSingleOutputExtensionCommonWild.
    def exitFugueSingleOutputExtensionCommonWild(self, ctx:fugue_sqlParser.FugueSingleOutputExtensionCommonWildContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSingleOutputExtensionCommon.
    def enterFugueSingleOutputExtensionCommon(self, ctx:fugue_sqlParser.FugueSingleOutputExtensionCommonContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSingleOutputExtensionCommon.
    def exitFugueSingleOutputExtensionCommon(self, ctx:fugue_sqlParser.FugueSingleOutputExtensionCommonContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueExtension.
    def enterFugueExtension(self, ctx:fugue_sqlParser.FugueExtensionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueExtension.
    def exitFugueExtension(self, ctx:fugue_sqlParser.FugueExtensionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueZipType.
    def enterFugueZipType(self, ctx:fugue_sqlParser.FugueZipTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueZipType.
    def exitFugueZipType(self, ctx:fugue_sqlParser.FugueZipTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fuguePrepartition.
    def enterFuguePrepartition(self, ctx:fugue_sqlParser.FuguePrepartitionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fuguePrepartition.
    def exitFuguePrepartition(self, ctx:fugue_sqlParser.FuguePrepartitionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fuguePartitionAlgo.
    def enterFuguePartitionAlgo(self, ctx:fugue_sqlParser.FuguePartitionAlgoContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fuguePartitionAlgo.
    def exitFuguePartitionAlgo(self, ctx:fugue_sqlParser.FuguePartitionAlgoContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fuguePartitionNum.
    def enterFuguePartitionNum(self, ctx:fugue_sqlParser.FuguePartitionNumContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fuguePartitionNum.
    def exitFuguePartitionNum(self, ctx:fugue_sqlParser.FuguePartitionNumContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fuguePartitionNumber.
    def enterFuguePartitionNumber(self, ctx:fugue_sqlParser.FuguePartitionNumberContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fuguePartitionNumber.
    def exitFuguePartitionNumber(self, ctx:fugue_sqlParser.FuguePartitionNumberContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueParamsPairs.
    def enterFugueParamsPairs(self, ctx:fugue_sqlParser.FugueParamsPairsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueParamsPairs.
    def exitFugueParamsPairs(self, ctx:fugue_sqlParser.FugueParamsPairsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueParamsObj.
    def enterFugueParamsObj(self, ctx:fugue_sqlParser.FugueParamsObjContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueParamsObj.
    def exitFugueParamsObj(self, ctx:fugue_sqlParser.FugueParamsObjContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueCols.
    def enterFugueCols(self, ctx:fugue_sqlParser.FugueColsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueCols.
    def exitFugueCols(self, ctx:fugue_sqlParser.FugueColsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueColsSort.
    def enterFugueColsSort(self, ctx:fugue_sqlParser.FugueColsSortContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueColsSort.
    def exitFugueColsSort(self, ctx:fugue_sqlParser.FugueColsSortContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueColSort.
    def enterFugueColSort(self, ctx:fugue_sqlParser.FugueColSortContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueColSort.
    def exitFugueColSort(self, ctx:fugue_sqlParser.FugueColSortContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueColumnIdentifier.
    def enterFugueColumnIdentifier(self, ctx:fugue_sqlParser.FugueColumnIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueColumnIdentifier.
    def exitFugueColumnIdentifier(self, ctx:fugue_sqlParser.FugueColumnIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueWildSchema.
    def enterFugueWildSchema(self, ctx:fugue_sqlParser.FugueWildSchemaContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueWildSchema.
    def exitFugueWildSchema(self, ctx:fugue_sqlParser.FugueWildSchemaContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueWildSchemaPair.
    def enterFugueWildSchemaPair(self, ctx:fugue_sqlParser.FugueWildSchemaPairContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueWildSchemaPair.
    def exitFugueWildSchemaPair(self, ctx:fugue_sqlParser.FugueWildSchemaPairContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSchema.
    def enterFugueSchema(self, ctx:fugue_sqlParser.FugueSchemaContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSchema.
    def exitFugueSchema(self, ctx:fugue_sqlParser.FugueSchemaContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSchemaPair.
    def enterFugueSchemaPair(self, ctx:fugue_sqlParser.FugueSchemaPairContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSchemaPair.
    def exitFugueSchemaPair(self, ctx:fugue_sqlParser.FugueSchemaPairContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSchemaKey.
    def enterFugueSchemaKey(self, ctx:fugue_sqlParser.FugueSchemaKeyContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSchemaKey.
    def exitFugueSchemaKey(self, ctx:fugue_sqlParser.FugueSchemaKeyContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSchemaSimpleType.
    def enterFugueSchemaSimpleType(self, ctx:fugue_sqlParser.FugueSchemaSimpleTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSchemaSimpleType.
    def exitFugueSchemaSimpleType(self, ctx:fugue_sqlParser.FugueSchemaSimpleTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSchemaListType.
    def enterFugueSchemaListType(self, ctx:fugue_sqlParser.FugueSchemaListTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSchemaListType.
    def exitFugueSchemaListType(self, ctx:fugue_sqlParser.FugueSchemaListTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueSchemaStructType.
    def enterFugueSchemaStructType(self, ctx:fugue_sqlParser.FugueSchemaStructTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueSchemaStructType.
    def exitFugueSchemaStructType(self, ctx:fugue_sqlParser.FugueSchemaStructTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJson.
    def enterFugueJson(self, ctx:fugue_sqlParser.FugueJsonContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJson.
    def exitFugueJson(self, ctx:fugue_sqlParser.FugueJsonContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonObj.
    def enterFugueJsonObj(self, ctx:fugue_sqlParser.FugueJsonObjContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonObj.
    def exitFugueJsonObj(self, ctx:fugue_sqlParser.FugueJsonObjContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonPairs.
    def enterFugueJsonPairs(self, ctx:fugue_sqlParser.FugueJsonPairsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonPairs.
    def exitFugueJsonPairs(self, ctx:fugue_sqlParser.FugueJsonPairsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonPair.
    def enterFugueJsonPair(self, ctx:fugue_sqlParser.FugueJsonPairContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonPair.
    def exitFugueJsonPair(self, ctx:fugue_sqlParser.FugueJsonPairContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonKey.
    def enterFugueJsonKey(self, ctx:fugue_sqlParser.FugueJsonKeyContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonKey.
    def exitFugueJsonKey(self, ctx:fugue_sqlParser.FugueJsonKeyContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonArray.
    def enterFugueJsonArray(self, ctx:fugue_sqlParser.FugueJsonArrayContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonArray.
    def exitFugueJsonArray(self, ctx:fugue_sqlParser.FugueJsonArrayContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonValue.
    def enterFugueJsonValue(self, ctx:fugue_sqlParser.FugueJsonValueContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonValue.
    def exitFugueJsonValue(self, ctx:fugue_sqlParser.FugueJsonValueContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonNumber.
    def enterFugueJsonNumber(self, ctx:fugue_sqlParser.FugueJsonNumberContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonNumber.
    def exitFugueJsonNumber(self, ctx:fugue_sqlParser.FugueJsonNumberContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonString.
    def enterFugueJsonString(self, ctx:fugue_sqlParser.FugueJsonStringContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonString.
    def exitFugueJsonString(self, ctx:fugue_sqlParser.FugueJsonStringContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonBool.
    def enterFugueJsonBool(self, ctx:fugue_sqlParser.FugueJsonBoolContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonBool.
    def exitFugueJsonBool(self, ctx:fugue_sqlParser.FugueJsonBoolContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueJsonNull.
    def enterFugueJsonNull(self, ctx:fugue_sqlParser.FugueJsonNullContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueJsonNull.
    def exitFugueJsonNull(self, ctx:fugue_sqlParser.FugueJsonNullContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fugueIdentifier.
    def enterFugueIdentifier(self, ctx:fugue_sqlParser.FugueIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fugueIdentifier.
    def exitFugueIdentifier(self, ctx:fugue_sqlParser.FugueIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#singleStatement.
    def enterSingleStatement(self, ctx:fugue_sqlParser.SingleStatementContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#singleStatement.
    def exitSingleStatement(self, ctx:fugue_sqlParser.SingleStatementContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#singleExpression.
    def enterSingleExpression(self, ctx:fugue_sqlParser.SingleExpressionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#singleExpression.
    def exitSingleExpression(self, ctx:fugue_sqlParser.SingleExpressionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#singleTableIdentifier.
    def enterSingleTableIdentifier(self, ctx:fugue_sqlParser.SingleTableIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#singleTableIdentifier.
    def exitSingleTableIdentifier(self, ctx:fugue_sqlParser.SingleTableIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#singleMultipartIdentifier.
    def enterSingleMultipartIdentifier(self, ctx:fugue_sqlParser.SingleMultipartIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#singleMultipartIdentifier.
    def exitSingleMultipartIdentifier(self, ctx:fugue_sqlParser.SingleMultipartIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#singleFunctionIdentifier.
    def enterSingleFunctionIdentifier(self, ctx:fugue_sqlParser.SingleFunctionIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#singleFunctionIdentifier.
    def exitSingleFunctionIdentifier(self, ctx:fugue_sqlParser.SingleFunctionIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#singleDataType.
    def enterSingleDataType(self, ctx:fugue_sqlParser.SingleDataTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#singleDataType.
    def exitSingleDataType(self, ctx:fugue_sqlParser.SingleDataTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#singleTableSchema.
    def enterSingleTableSchema(self, ctx:fugue_sqlParser.SingleTableSchemaContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#singleTableSchema.
    def exitSingleTableSchema(self, ctx:fugue_sqlParser.SingleTableSchemaContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#statementDefault.
    def enterStatementDefault(self, ctx:fugue_sqlParser.StatementDefaultContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#statementDefault.
    def exitStatementDefault(self, ctx:fugue_sqlParser.StatementDefaultContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#dmlStatement.
    def enterDmlStatement(self, ctx:fugue_sqlParser.DmlStatementContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#dmlStatement.
    def exitDmlStatement(self, ctx:fugue_sqlParser.DmlStatementContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#use.
    def enterUse(self, ctx:fugue_sqlParser.UseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#use.
    def exitUse(self, ctx:fugue_sqlParser.UseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createNamespace.
    def enterCreateNamespace(self, ctx:fugue_sqlParser.CreateNamespaceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createNamespace.
    def exitCreateNamespace(self, ctx:fugue_sqlParser.CreateNamespaceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#setNamespaceProperties.
    def enterSetNamespaceProperties(self, ctx:fugue_sqlParser.SetNamespacePropertiesContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#setNamespaceProperties.
    def exitSetNamespaceProperties(self, ctx:fugue_sqlParser.SetNamespacePropertiesContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#setNamespaceLocation.
    def enterSetNamespaceLocation(self, ctx:fugue_sqlParser.SetNamespaceLocationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#setNamespaceLocation.
    def exitSetNamespaceLocation(self, ctx:fugue_sqlParser.SetNamespaceLocationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#dropNamespace.
    def enterDropNamespace(self, ctx:fugue_sqlParser.DropNamespaceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#dropNamespace.
    def exitDropNamespace(self, ctx:fugue_sqlParser.DropNamespaceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showNamespaces.
    def enterShowNamespaces(self, ctx:fugue_sqlParser.ShowNamespacesContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showNamespaces.
    def exitShowNamespaces(self, ctx:fugue_sqlParser.ShowNamespacesContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createTable.
    def enterCreateTable(self, ctx:fugue_sqlParser.CreateTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createTable.
    def exitCreateTable(self, ctx:fugue_sqlParser.CreateTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createHiveTable.
    def enterCreateHiveTable(self, ctx:fugue_sqlParser.CreateHiveTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createHiveTable.
    def exitCreateHiveTable(self, ctx:fugue_sqlParser.CreateHiveTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createTableLike.
    def enterCreateTableLike(self, ctx:fugue_sqlParser.CreateTableLikeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createTableLike.
    def exitCreateTableLike(self, ctx:fugue_sqlParser.CreateTableLikeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#replaceTable.
    def enterReplaceTable(self, ctx:fugue_sqlParser.ReplaceTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#replaceTable.
    def exitReplaceTable(self, ctx:fugue_sqlParser.ReplaceTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#analyze.
    def enterAnalyze(self, ctx:fugue_sqlParser.AnalyzeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#analyze.
    def exitAnalyze(self, ctx:fugue_sqlParser.AnalyzeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#addTableColumns.
    def enterAddTableColumns(self, ctx:fugue_sqlParser.AddTableColumnsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#addTableColumns.
    def exitAddTableColumns(self, ctx:fugue_sqlParser.AddTableColumnsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#renameTableColumn.
    def enterRenameTableColumn(self, ctx:fugue_sqlParser.RenameTableColumnContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#renameTableColumn.
    def exitRenameTableColumn(self, ctx:fugue_sqlParser.RenameTableColumnContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#dropTableColumns.
    def enterDropTableColumns(self, ctx:fugue_sqlParser.DropTableColumnsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#dropTableColumns.
    def exitDropTableColumns(self, ctx:fugue_sqlParser.DropTableColumnsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#renameTable.
    def enterRenameTable(self, ctx:fugue_sqlParser.RenameTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#renameTable.
    def exitRenameTable(self, ctx:fugue_sqlParser.RenameTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#setTableProperties.
    def enterSetTableProperties(self, ctx:fugue_sqlParser.SetTablePropertiesContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#setTableProperties.
    def exitSetTableProperties(self, ctx:fugue_sqlParser.SetTablePropertiesContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#unsetTableProperties.
    def enterUnsetTableProperties(self, ctx:fugue_sqlParser.UnsetTablePropertiesContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#unsetTableProperties.
    def exitUnsetTableProperties(self, ctx:fugue_sqlParser.UnsetTablePropertiesContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#alterTableAlterColumn.
    def enterAlterTableAlterColumn(self, ctx:fugue_sqlParser.AlterTableAlterColumnContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#alterTableAlterColumn.
    def exitAlterTableAlterColumn(self, ctx:fugue_sqlParser.AlterTableAlterColumnContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#hiveChangeColumn.
    def enterHiveChangeColumn(self, ctx:fugue_sqlParser.HiveChangeColumnContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#hiveChangeColumn.
    def exitHiveChangeColumn(self, ctx:fugue_sqlParser.HiveChangeColumnContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#hiveReplaceColumns.
    def enterHiveReplaceColumns(self, ctx:fugue_sqlParser.HiveReplaceColumnsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#hiveReplaceColumns.
    def exitHiveReplaceColumns(self, ctx:fugue_sqlParser.HiveReplaceColumnsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#setTableSerDe.
    def enterSetTableSerDe(self, ctx:fugue_sqlParser.SetTableSerDeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#setTableSerDe.
    def exitSetTableSerDe(self, ctx:fugue_sqlParser.SetTableSerDeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#addTablePartition.
    def enterAddTablePartition(self, ctx:fugue_sqlParser.AddTablePartitionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#addTablePartition.
    def exitAddTablePartition(self, ctx:fugue_sqlParser.AddTablePartitionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#renameTablePartition.
    def enterRenameTablePartition(self, ctx:fugue_sqlParser.RenameTablePartitionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#renameTablePartition.
    def exitRenameTablePartition(self, ctx:fugue_sqlParser.RenameTablePartitionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#dropTablePartitions.
    def enterDropTablePartitions(self, ctx:fugue_sqlParser.DropTablePartitionsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#dropTablePartitions.
    def exitDropTablePartitions(self, ctx:fugue_sqlParser.DropTablePartitionsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#setTableLocation.
    def enterSetTableLocation(self, ctx:fugue_sqlParser.SetTableLocationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#setTableLocation.
    def exitSetTableLocation(self, ctx:fugue_sqlParser.SetTableLocationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#recoverPartitions.
    def enterRecoverPartitions(self, ctx:fugue_sqlParser.RecoverPartitionsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#recoverPartitions.
    def exitRecoverPartitions(self, ctx:fugue_sqlParser.RecoverPartitionsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#dropTable.
    def enterDropTable(self, ctx:fugue_sqlParser.DropTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#dropTable.
    def exitDropTable(self, ctx:fugue_sqlParser.DropTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#dropView.
    def enterDropView(self, ctx:fugue_sqlParser.DropViewContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#dropView.
    def exitDropView(self, ctx:fugue_sqlParser.DropViewContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createView.
    def enterCreateView(self, ctx:fugue_sqlParser.CreateViewContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createView.
    def exitCreateView(self, ctx:fugue_sqlParser.CreateViewContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createTempViewUsing.
    def enterCreateTempViewUsing(self, ctx:fugue_sqlParser.CreateTempViewUsingContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createTempViewUsing.
    def exitCreateTempViewUsing(self, ctx:fugue_sqlParser.CreateTempViewUsingContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#alterViewQuery.
    def enterAlterViewQuery(self, ctx:fugue_sqlParser.AlterViewQueryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#alterViewQuery.
    def exitAlterViewQuery(self, ctx:fugue_sqlParser.AlterViewQueryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createFunction.
    def enterCreateFunction(self, ctx:fugue_sqlParser.CreateFunctionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createFunction.
    def exitCreateFunction(self, ctx:fugue_sqlParser.CreateFunctionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#dropFunction.
    def enterDropFunction(self, ctx:fugue_sqlParser.DropFunctionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#dropFunction.
    def exitDropFunction(self, ctx:fugue_sqlParser.DropFunctionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#explain.
    def enterExplain(self, ctx:fugue_sqlParser.ExplainContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#explain.
    def exitExplain(self, ctx:fugue_sqlParser.ExplainContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showTables.
    def enterShowTables(self, ctx:fugue_sqlParser.ShowTablesContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showTables.
    def exitShowTables(self, ctx:fugue_sqlParser.ShowTablesContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showTable.
    def enterShowTable(self, ctx:fugue_sqlParser.ShowTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showTable.
    def exitShowTable(self, ctx:fugue_sqlParser.ShowTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showTblProperties.
    def enterShowTblProperties(self, ctx:fugue_sqlParser.ShowTblPropertiesContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showTblProperties.
    def exitShowTblProperties(self, ctx:fugue_sqlParser.ShowTblPropertiesContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showColumns.
    def enterShowColumns(self, ctx:fugue_sqlParser.ShowColumnsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showColumns.
    def exitShowColumns(self, ctx:fugue_sqlParser.ShowColumnsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showViews.
    def enterShowViews(self, ctx:fugue_sqlParser.ShowViewsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showViews.
    def exitShowViews(self, ctx:fugue_sqlParser.ShowViewsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showPartitions.
    def enterShowPartitions(self, ctx:fugue_sqlParser.ShowPartitionsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showPartitions.
    def exitShowPartitions(self, ctx:fugue_sqlParser.ShowPartitionsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showFunctions.
    def enterShowFunctions(self, ctx:fugue_sqlParser.ShowFunctionsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showFunctions.
    def exitShowFunctions(self, ctx:fugue_sqlParser.ShowFunctionsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showCreateTable.
    def enterShowCreateTable(self, ctx:fugue_sqlParser.ShowCreateTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showCreateTable.
    def exitShowCreateTable(self, ctx:fugue_sqlParser.ShowCreateTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#showCurrentNamespace.
    def enterShowCurrentNamespace(self, ctx:fugue_sqlParser.ShowCurrentNamespaceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#showCurrentNamespace.
    def exitShowCurrentNamespace(self, ctx:fugue_sqlParser.ShowCurrentNamespaceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#describeFunction.
    def enterDescribeFunction(self, ctx:fugue_sqlParser.DescribeFunctionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#describeFunction.
    def exitDescribeFunction(self, ctx:fugue_sqlParser.DescribeFunctionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#describeNamespace.
    def enterDescribeNamespace(self, ctx:fugue_sqlParser.DescribeNamespaceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#describeNamespace.
    def exitDescribeNamespace(self, ctx:fugue_sqlParser.DescribeNamespaceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#describeRelation.
    def enterDescribeRelation(self, ctx:fugue_sqlParser.DescribeRelationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#describeRelation.
    def exitDescribeRelation(self, ctx:fugue_sqlParser.DescribeRelationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#describeQuery.
    def enterDescribeQuery(self, ctx:fugue_sqlParser.DescribeQueryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#describeQuery.
    def exitDescribeQuery(self, ctx:fugue_sqlParser.DescribeQueryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#commentNamespace.
    def enterCommentNamespace(self, ctx:fugue_sqlParser.CommentNamespaceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#commentNamespace.
    def exitCommentNamespace(self, ctx:fugue_sqlParser.CommentNamespaceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#commentTable.
    def enterCommentTable(self, ctx:fugue_sqlParser.CommentTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#commentTable.
    def exitCommentTable(self, ctx:fugue_sqlParser.CommentTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#refreshTable.
    def enterRefreshTable(self, ctx:fugue_sqlParser.RefreshTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#refreshTable.
    def exitRefreshTable(self, ctx:fugue_sqlParser.RefreshTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#refreshResource.
    def enterRefreshResource(self, ctx:fugue_sqlParser.RefreshResourceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#refreshResource.
    def exitRefreshResource(self, ctx:fugue_sqlParser.RefreshResourceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#cacheTable.
    def enterCacheTable(self, ctx:fugue_sqlParser.CacheTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#cacheTable.
    def exitCacheTable(self, ctx:fugue_sqlParser.CacheTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#uncacheTable.
    def enterUncacheTable(self, ctx:fugue_sqlParser.UncacheTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#uncacheTable.
    def exitUncacheTable(self, ctx:fugue_sqlParser.UncacheTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#clearCache.
    def enterClearCache(self, ctx:fugue_sqlParser.ClearCacheContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#clearCache.
    def exitClearCache(self, ctx:fugue_sqlParser.ClearCacheContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#loadData.
    def enterLoadData(self, ctx:fugue_sqlParser.LoadDataContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#loadData.
    def exitLoadData(self, ctx:fugue_sqlParser.LoadDataContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#truncateTable.
    def enterTruncateTable(self, ctx:fugue_sqlParser.TruncateTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#truncateTable.
    def exitTruncateTable(self, ctx:fugue_sqlParser.TruncateTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#repairTable.
    def enterRepairTable(self, ctx:fugue_sqlParser.RepairTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#repairTable.
    def exitRepairTable(self, ctx:fugue_sqlParser.RepairTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#manageResource.
    def enterManageResource(self, ctx:fugue_sqlParser.ManageResourceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#manageResource.
    def exitManageResource(self, ctx:fugue_sqlParser.ManageResourceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#failNativeCommand.
    def enterFailNativeCommand(self, ctx:fugue_sqlParser.FailNativeCommandContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#failNativeCommand.
    def exitFailNativeCommand(self, ctx:fugue_sqlParser.FailNativeCommandContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#setConfiguration.
    def enterSetConfiguration(self, ctx:fugue_sqlParser.SetConfigurationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#setConfiguration.
    def exitSetConfiguration(self, ctx:fugue_sqlParser.SetConfigurationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#resetConfiguration.
    def enterResetConfiguration(self, ctx:fugue_sqlParser.ResetConfigurationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#resetConfiguration.
    def exitResetConfiguration(self, ctx:fugue_sqlParser.ResetConfigurationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#unsupportedHiveNativeCommands.
    def enterUnsupportedHiveNativeCommands(self, ctx:fugue_sqlParser.UnsupportedHiveNativeCommandsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#unsupportedHiveNativeCommands.
    def exitUnsupportedHiveNativeCommands(self, ctx:fugue_sqlParser.UnsupportedHiveNativeCommandsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createTableHeader.
    def enterCreateTableHeader(self, ctx:fugue_sqlParser.CreateTableHeaderContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createTableHeader.
    def exitCreateTableHeader(self, ctx:fugue_sqlParser.CreateTableHeaderContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#replaceTableHeader.
    def enterReplaceTableHeader(self, ctx:fugue_sqlParser.ReplaceTableHeaderContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#replaceTableHeader.
    def exitReplaceTableHeader(self, ctx:fugue_sqlParser.ReplaceTableHeaderContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#bucketSpec.
    def enterBucketSpec(self, ctx:fugue_sqlParser.BucketSpecContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#bucketSpec.
    def exitBucketSpec(self, ctx:fugue_sqlParser.BucketSpecContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#skewSpec.
    def enterSkewSpec(self, ctx:fugue_sqlParser.SkewSpecContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#skewSpec.
    def exitSkewSpec(self, ctx:fugue_sqlParser.SkewSpecContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#locationSpec.
    def enterLocationSpec(self, ctx:fugue_sqlParser.LocationSpecContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#locationSpec.
    def exitLocationSpec(self, ctx:fugue_sqlParser.LocationSpecContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#commentSpec.
    def enterCommentSpec(self, ctx:fugue_sqlParser.CommentSpecContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#commentSpec.
    def exitCommentSpec(self, ctx:fugue_sqlParser.CommentSpecContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#query.
    def enterQuery(self, ctx:fugue_sqlParser.QueryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#query.
    def exitQuery(self, ctx:fugue_sqlParser.QueryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#insertOverwriteTable.
    def enterInsertOverwriteTable(self, ctx:fugue_sqlParser.InsertOverwriteTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#insertOverwriteTable.
    def exitInsertOverwriteTable(self, ctx:fugue_sqlParser.InsertOverwriteTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#insertIntoTable.
    def enterInsertIntoTable(self, ctx:fugue_sqlParser.InsertIntoTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#insertIntoTable.
    def exitInsertIntoTable(self, ctx:fugue_sqlParser.InsertIntoTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#insertOverwriteHiveDir.
    def enterInsertOverwriteHiveDir(self, ctx:fugue_sqlParser.InsertOverwriteHiveDirContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#insertOverwriteHiveDir.
    def exitInsertOverwriteHiveDir(self, ctx:fugue_sqlParser.InsertOverwriteHiveDirContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#insertOverwriteDir.
    def enterInsertOverwriteDir(self, ctx:fugue_sqlParser.InsertOverwriteDirContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#insertOverwriteDir.
    def exitInsertOverwriteDir(self, ctx:fugue_sqlParser.InsertOverwriteDirContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#partitionSpecLocation.
    def enterPartitionSpecLocation(self, ctx:fugue_sqlParser.PartitionSpecLocationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#partitionSpecLocation.
    def exitPartitionSpecLocation(self, ctx:fugue_sqlParser.PartitionSpecLocationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#partitionSpec.
    def enterPartitionSpec(self, ctx:fugue_sqlParser.PartitionSpecContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#partitionSpec.
    def exitPartitionSpec(self, ctx:fugue_sqlParser.PartitionSpecContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#partitionVal.
    def enterPartitionVal(self, ctx:fugue_sqlParser.PartitionValContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#partitionVal.
    def exitPartitionVal(self, ctx:fugue_sqlParser.PartitionValContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#namespace.
    def enterNamespace(self, ctx:fugue_sqlParser.NamespaceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#namespace.
    def exitNamespace(self, ctx:fugue_sqlParser.NamespaceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#describeFuncName.
    def enterDescribeFuncName(self, ctx:fugue_sqlParser.DescribeFuncNameContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#describeFuncName.
    def exitDescribeFuncName(self, ctx:fugue_sqlParser.DescribeFuncNameContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#describeColName.
    def enterDescribeColName(self, ctx:fugue_sqlParser.DescribeColNameContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#describeColName.
    def exitDescribeColName(self, ctx:fugue_sqlParser.DescribeColNameContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#ctes.
    def enterCtes(self, ctx:fugue_sqlParser.CtesContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#ctes.
    def exitCtes(self, ctx:fugue_sqlParser.CtesContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#namedQuery.
    def enterNamedQuery(self, ctx:fugue_sqlParser.NamedQueryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#namedQuery.
    def exitNamedQuery(self, ctx:fugue_sqlParser.NamedQueryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tableProvider.
    def enterTableProvider(self, ctx:fugue_sqlParser.TableProviderContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tableProvider.
    def exitTableProvider(self, ctx:fugue_sqlParser.TableProviderContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createTableClauses.
    def enterCreateTableClauses(self, ctx:fugue_sqlParser.CreateTableClausesContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createTableClauses.
    def exitCreateTableClauses(self, ctx:fugue_sqlParser.CreateTableClausesContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tablePropertyList.
    def enterTablePropertyList(self, ctx:fugue_sqlParser.TablePropertyListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tablePropertyList.
    def exitTablePropertyList(self, ctx:fugue_sqlParser.TablePropertyListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tableProperty.
    def enterTableProperty(self, ctx:fugue_sqlParser.TablePropertyContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tableProperty.
    def exitTableProperty(self, ctx:fugue_sqlParser.TablePropertyContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tablePropertyKey.
    def enterTablePropertyKey(self, ctx:fugue_sqlParser.TablePropertyKeyContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tablePropertyKey.
    def exitTablePropertyKey(self, ctx:fugue_sqlParser.TablePropertyKeyContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tablePropertyValue.
    def enterTablePropertyValue(self, ctx:fugue_sqlParser.TablePropertyValueContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tablePropertyValue.
    def exitTablePropertyValue(self, ctx:fugue_sqlParser.TablePropertyValueContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#constantList.
    def enterConstantList(self, ctx:fugue_sqlParser.ConstantListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#constantList.
    def exitConstantList(self, ctx:fugue_sqlParser.ConstantListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#nestedConstantList.
    def enterNestedConstantList(self, ctx:fugue_sqlParser.NestedConstantListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#nestedConstantList.
    def exitNestedConstantList(self, ctx:fugue_sqlParser.NestedConstantListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#createFileFormat.
    def enterCreateFileFormat(self, ctx:fugue_sqlParser.CreateFileFormatContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#createFileFormat.
    def exitCreateFileFormat(self, ctx:fugue_sqlParser.CreateFileFormatContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tableFileFormat.
    def enterTableFileFormat(self, ctx:fugue_sqlParser.TableFileFormatContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tableFileFormat.
    def exitTableFileFormat(self, ctx:fugue_sqlParser.TableFileFormatContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#genericFileFormat.
    def enterGenericFileFormat(self, ctx:fugue_sqlParser.GenericFileFormatContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#genericFileFormat.
    def exitGenericFileFormat(self, ctx:fugue_sqlParser.GenericFileFormatContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#storageHandler.
    def enterStorageHandler(self, ctx:fugue_sqlParser.StorageHandlerContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#storageHandler.
    def exitStorageHandler(self, ctx:fugue_sqlParser.StorageHandlerContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#resource.
    def enterResource(self, ctx:fugue_sqlParser.ResourceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#resource.
    def exitResource(self, ctx:fugue_sqlParser.ResourceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#singleInsertQuery.
    def enterSingleInsertQuery(self, ctx:fugue_sqlParser.SingleInsertQueryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#singleInsertQuery.
    def exitSingleInsertQuery(self, ctx:fugue_sqlParser.SingleInsertQueryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#multiInsertQuery.
    def enterMultiInsertQuery(self, ctx:fugue_sqlParser.MultiInsertQueryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#multiInsertQuery.
    def exitMultiInsertQuery(self, ctx:fugue_sqlParser.MultiInsertQueryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#deleteFromTable.
    def enterDeleteFromTable(self, ctx:fugue_sqlParser.DeleteFromTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#deleteFromTable.
    def exitDeleteFromTable(self, ctx:fugue_sqlParser.DeleteFromTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#updateTable.
    def enterUpdateTable(self, ctx:fugue_sqlParser.UpdateTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#updateTable.
    def exitUpdateTable(self, ctx:fugue_sqlParser.UpdateTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#mergeIntoTable.
    def enterMergeIntoTable(self, ctx:fugue_sqlParser.MergeIntoTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#mergeIntoTable.
    def exitMergeIntoTable(self, ctx:fugue_sqlParser.MergeIntoTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#queryOrganization.
    def enterQueryOrganization(self, ctx:fugue_sqlParser.QueryOrganizationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#queryOrganization.
    def exitQueryOrganization(self, ctx:fugue_sqlParser.QueryOrganizationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#multiInsertQueryBody.
    def enterMultiInsertQueryBody(self, ctx:fugue_sqlParser.MultiInsertQueryBodyContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#multiInsertQueryBody.
    def exitMultiInsertQueryBody(self, ctx:fugue_sqlParser.MultiInsertQueryBodyContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#queryTermDefault.
    def enterQueryTermDefault(self, ctx:fugue_sqlParser.QueryTermDefaultContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#queryTermDefault.
    def exitQueryTermDefault(self, ctx:fugue_sqlParser.QueryTermDefaultContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#setOperation.
    def enterSetOperation(self, ctx:fugue_sqlParser.SetOperationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#setOperation.
    def exitSetOperation(self, ctx:fugue_sqlParser.SetOperationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#queryPrimaryDefault.
    def enterQueryPrimaryDefault(self, ctx:fugue_sqlParser.QueryPrimaryDefaultContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#queryPrimaryDefault.
    def exitQueryPrimaryDefault(self, ctx:fugue_sqlParser.QueryPrimaryDefaultContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fromStmt.
    def enterFromStmt(self, ctx:fugue_sqlParser.FromStmtContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fromStmt.
    def exitFromStmt(self, ctx:fugue_sqlParser.FromStmtContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#table.
    def enterTable(self, ctx:fugue_sqlParser.TableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#table.
    def exitTable(self, ctx:fugue_sqlParser.TableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#inlineTableDefault1.
    def enterInlineTableDefault1(self, ctx:fugue_sqlParser.InlineTableDefault1Context):
        pass

    # Exit a parse tree produced by fugue_sqlParser#inlineTableDefault1.
    def exitInlineTableDefault1(self, ctx:fugue_sqlParser.InlineTableDefault1Context):
        pass


    # Enter a parse tree produced by fugue_sqlParser#sortItem.
    def enterSortItem(self, ctx:fugue_sqlParser.SortItemContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#sortItem.
    def exitSortItem(self, ctx:fugue_sqlParser.SortItemContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fromStatement.
    def enterFromStatement(self, ctx:fugue_sqlParser.FromStatementContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fromStatement.
    def exitFromStatement(self, ctx:fugue_sqlParser.FromStatementContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fromStatementBody.
    def enterFromStatementBody(self, ctx:fugue_sqlParser.FromStatementBodyContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fromStatementBody.
    def exitFromStatementBody(self, ctx:fugue_sqlParser.FromStatementBodyContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#transformQuerySpecification.
    def enterTransformQuerySpecification(self, ctx:fugue_sqlParser.TransformQuerySpecificationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#transformQuerySpecification.
    def exitTransformQuerySpecification(self, ctx:fugue_sqlParser.TransformQuerySpecificationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#regularQuerySpecification.
    def enterRegularQuerySpecification(self, ctx:fugue_sqlParser.RegularQuerySpecificationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#regularQuerySpecification.
    def exitRegularQuerySpecification(self, ctx:fugue_sqlParser.RegularQuerySpecificationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#transformClause.
    def enterTransformClause(self, ctx:fugue_sqlParser.TransformClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#transformClause.
    def exitTransformClause(self, ctx:fugue_sqlParser.TransformClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#selectClause.
    def enterSelectClause(self, ctx:fugue_sqlParser.SelectClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#selectClause.
    def exitSelectClause(self, ctx:fugue_sqlParser.SelectClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#setClause.
    def enterSetClause(self, ctx:fugue_sqlParser.SetClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#setClause.
    def exitSetClause(self, ctx:fugue_sqlParser.SetClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#matchedClause.
    def enterMatchedClause(self, ctx:fugue_sqlParser.MatchedClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#matchedClause.
    def exitMatchedClause(self, ctx:fugue_sqlParser.MatchedClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#notMatchedClause.
    def enterNotMatchedClause(self, ctx:fugue_sqlParser.NotMatchedClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#notMatchedClause.
    def exitNotMatchedClause(self, ctx:fugue_sqlParser.NotMatchedClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#matchedAction.
    def enterMatchedAction(self, ctx:fugue_sqlParser.MatchedActionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#matchedAction.
    def exitMatchedAction(self, ctx:fugue_sqlParser.MatchedActionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#notMatchedAction.
    def enterNotMatchedAction(self, ctx:fugue_sqlParser.NotMatchedActionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#notMatchedAction.
    def exitNotMatchedAction(self, ctx:fugue_sqlParser.NotMatchedActionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#assignmentList.
    def enterAssignmentList(self, ctx:fugue_sqlParser.AssignmentListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#assignmentList.
    def exitAssignmentList(self, ctx:fugue_sqlParser.AssignmentListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#assignment.
    def enterAssignment(self, ctx:fugue_sqlParser.AssignmentContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#assignment.
    def exitAssignment(self, ctx:fugue_sqlParser.AssignmentContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#whereClause.
    def enterWhereClause(self, ctx:fugue_sqlParser.WhereClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#whereClause.
    def exitWhereClause(self, ctx:fugue_sqlParser.WhereClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#havingClause.
    def enterHavingClause(self, ctx:fugue_sqlParser.HavingClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#havingClause.
    def exitHavingClause(self, ctx:fugue_sqlParser.HavingClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#hint.
    def enterHint(self, ctx:fugue_sqlParser.HintContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#hint.
    def exitHint(self, ctx:fugue_sqlParser.HintContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#hintStatement.
    def enterHintStatement(self, ctx:fugue_sqlParser.HintStatementContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#hintStatement.
    def exitHintStatement(self, ctx:fugue_sqlParser.HintStatementContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#fromClause.
    def enterFromClause(self, ctx:fugue_sqlParser.FromClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#fromClause.
    def exitFromClause(self, ctx:fugue_sqlParser.FromClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#aggregationClause.
    def enterAggregationClause(self, ctx:fugue_sqlParser.AggregationClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#aggregationClause.
    def exitAggregationClause(self, ctx:fugue_sqlParser.AggregationClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#groupingSet.
    def enterGroupingSet(self, ctx:fugue_sqlParser.GroupingSetContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#groupingSet.
    def exitGroupingSet(self, ctx:fugue_sqlParser.GroupingSetContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#pivotClause.
    def enterPivotClause(self, ctx:fugue_sqlParser.PivotClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#pivotClause.
    def exitPivotClause(self, ctx:fugue_sqlParser.PivotClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#pivotColumn.
    def enterPivotColumn(self, ctx:fugue_sqlParser.PivotColumnContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#pivotColumn.
    def exitPivotColumn(self, ctx:fugue_sqlParser.PivotColumnContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#pivotValue.
    def enterPivotValue(self, ctx:fugue_sqlParser.PivotValueContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#pivotValue.
    def exitPivotValue(self, ctx:fugue_sqlParser.PivotValueContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#lateralView.
    def enterLateralView(self, ctx:fugue_sqlParser.LateralViewContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#lateralView.
    def exitLateralView(self, ctx:fugue_sqlParser.LateralViewContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#setQuantifier.
    def enterSetQuantifier(self, ctx:fugue_sqlParser.SetQuantifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#setQuantifier.
    def exitSetQuantifier(self, ctx:fugue_sqlParser.SetQuantifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#relation.
    def enterRelation(self, ctx:fugue_sqlParser.RelationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#relation.
    def exitRelation(self, ctx:fugue_sqlParser.RelationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#joinRelation.
    def enterJoinRelation(self, ctx:fugue_sqlParser.JoinRelationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#joinRelation.
    def exitJoinRelation(self, ctx:fugue_sqlParser.JoinRelationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#joinType.
    def enterJoinType(self, ctx:fugue_sqlParser.JoinTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#joinType.
    def exitJoinType(self, ctx:fugue_sqlParser.JoinTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#joinCriteria.
    def enterJoinCriteria(self, ctx:fugue_sqlParser.JoinCriteriaContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#joinCriteria.
    def exitJoinCriteria(self, ctx:fugue_sqlParser.JoinCriteriaContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#sample.
    def enterSample(self, ctx:fugue_sqlParser.SampleContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#sample.
    def exitSample(self, ctx:fugue_sqlParser.SampleContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#sampleByPercentile.
    def enterSampleByPercentile(self, ctx:fugue_sqlParser.SampleByPercentileContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#sampleByPercentile.
    def exitSampleByPercentile(self, ctx:fugue_sqlParser.SampleByPercentileContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#sampleByRows.
    def enterSampleByRows(self, ctx:fugue_sqlParser.SampleByRowsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#sampleByRows.
    def exitSampleByRows(self, ctx:fugue_sqlParser.SampleByRowsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#sampleByBucket.
    def enterSampleByBucket(self, ctx:fugue_sqlParser.SampleByBucketContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#sampleByBucket.
    def exitSampleByBucket(self, ctx:fugue_sqlParser.SampleByBucketContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#sampleByBytes.
    def enterSampleByBytes(self, ctx:fugue_sqlParser.SampleByBytesContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#sampleByBytes.
    def exitSampleByBytes(self, ctx:fugue_sqlParser.SampleByBytesContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#identifierList.
    def enterIdentifierList(self, ctx:fugue_sqlParser.IdentifierListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#identifierList.
    def exitIdentifierList(self, ctx:fugue_sqlParser.IdentifierListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#identifierSeq.
    def enterIdentifierSeq(self, ctx:fugue_sqlParser.IdentifierSeqContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#identifierSeq.
    def exitIdentifierSeq(self, ctx:fugue_sqlParser.IdentifierSeqContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#orderedIdentifierList.
    def enterOrderedIdentifierList(self, ctx:fugue_sqlParser.OrderedIdentifierListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#orderedIdentifierList.
    def exitOrderedIdentifierList(self, ctx:fugue_sqlParser.OrderedIdentifierListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#orderedIdentifier.
    def enterOrderedIdentifier(self, ctx:fugue_sqlParser.OrderedIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#orderedIdentifier.
    def exitOrderedIdentifier(self, ctx:fugue_sqlParser.OrderedIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#identifierCommentList.
    def enterIdentifierCommentList(self, ctx:fugue_sqlParser.IdentifierCommentListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#identifierCommentList.
    def exitIdentifierCommentList(self, ctx:fugue_sqlParser.IdentifierCommentListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#identifierComment.
    def enterIdentifierComment(self, ctx:fugue_sqlParser.IdentifierCommentContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#identifierComment.
    def exitIdentifierComment(self, ctx:fugue_sqlParser.IdentifierCommentContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tableName.
    def enterTableName(self, ctx:fugue_sqlParser.TableNameContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tableName.
    def exitTableName(self, ctx:fugue_sqlParser.TableNameContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#aliasedQuery.
    def enterAliasedQuery(self, ctx:fugue_sqlParser.AliasedQueryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#aliasedQuery.
    def exitAliasedQuery(self, ctx:fugue_sqlParser.AliasedQueryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#aliasedFugueNested.
    def enterAliasedFugueNested(self, ctx:fugue_sqlParser.AliasedFugueNestedContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#aliasedFugueNested.
    def exitAliasedFugueNested(self, ctx:fugue_sqlParser.AliasedFugueNestedContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#aliasedRelation.
    def enterAliasedRelation(self, ctx:fugue_sqlParser.AliasedRelationContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#aliasedRelation.
    def exitAliasedRelation(self, ctx:fugue_sqlParser.AliasedRelationContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#inlineTableDefault2.
    def enterInlineTableDefault2(self, ctx:fugue_sqlParser.InlineTableDefault2Context):
        pass

    # Exit a parse tree produced by fugue_sqlParser#inlineTableDefault2.
    def exitInlineTableDefault2(self, ctx:fugue_sqlParser.InlineTableDefault2Context):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tableValuedFunction.
    def enterTableValuedFunction(self, ctx:fugue_sqlParser.TableValuedFunctionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tableValuedFunction.
    def exitTableValuedFunction(self, ctx:fugue_sqlParser.TableValuedFunctionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#inlineTable.
    def enterInlineTable(self, ctx:fugue_sqlParser.InlineTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#inlineTable.
    def exitInlineTable(self, ctx:fugue_sqlParser.InlineTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#functionTable.
    def enterFunctionTable(self, ctx:fugue_sqlParser.FunctionTableContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#functionTable.
    def exitFunctionTable(self, ctx:fugue_sqlParser.FunctionTableContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tableAlias.
    def enterTableAlias(self, ctx:fugue_sqlParser.TableAliasContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tableAlias.
    def exitTableAlias(self, ctx:fugue_sqlParser.TableAliasContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#rowFormatSerde.
    def enterRowFormatSerde(self, ctx:fugue_sqlParser.RowFormatSerdeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#rowFormatSerde.
    def exitRowFormatSerde(self, ctx:fugue_sqlParser.RowFormatSerdeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#rowFormatDelimited.
    def enterRowFormatDelimited(self, ctx:fugue_sqlParser.RowFormatDelimitedContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#rowFormatDelimited.
    def exitRowFormatDelimited(self, ctx:fugue_sqlParser.RowFormatDelimitedContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#multipartIdentifierList.
    def enterMultipartIdentifierList(self, ctx:fugue_sqlParser.MultipartIdentifierListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#multipartIdentifierList.
    def exitMultipartIdentifierList(self, ctx:fugue_sqlParser.MultipartIdentifierListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#multipartIdentifier.
    def enterMultipartIdentifier(self, ctx:fugue_sqlParser.MultipartIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#multipartIdentifier.
    def exitMultipartIdentifier(self, ctx:fugue_sqlParser.MultipartIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tableIdentifier.
    def enterTableIdentifier(self, ctx:fugue_sqlParser.TableIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tableIdentifier.
    def exitTableIdentifier(self, ctx:fugue_sqlParser.TableIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#functionIdentifier.
    def enterFunctionIdentifier(self, ctx:fugue_sqlParser.FunctionIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#functionIdentifier.
    def exitFunctionIdentifier(self, ctx:fugue_sqlParser.FunctionIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#namedExpression.
    def enterNamedExpression(self, ctx:fugue_sqlParser.NamedExpressionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#namedExpression.
    def exitNamedExpression(self, ctx:fugue_sqlParser.NamedExpressionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#namedExpressionSeq.
    def enterNamedExpressionSeq(self, ctx:fugue_sqlParser.NamedExpressionSeqContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#namedExpressionSeq.
    def exitNamedExpressionSeq(self, ctx:fugue_sqlParser.NamedExpressionSeqContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#transformList.
    def enterTransformList(self, ctx:fugue_sqlParser.TransformListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#transformList.
    def exitTransformList(self, ctx:fugue_sqlParser.TransformListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#identityTransform.
    def enterIdentityTransform(self, ctx:fugue_sqlParser.IdentityTransformContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#identityTransform.
    def exitIdentityTransform(self, ctx:fugue_sqlParser.IdentityTransformContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#applyTransform.
    def enterApplyTransform(self, ctx:fugue_sqlParser.ApplyTransformContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#applyTransform.
    def exitApplyTransform(self, ctx:fugue_sqlParser.ApplyTransformContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#transformArgument.
    def enterTransformArgument(self, ctx:fugue_sqlParser.TransformArgumentContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#transformArgument.
    def exitTransformArgument(self, ctx:fugue_sqlParser.TransformArgumentContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#expression.
    def enterExpression(self, ctx:fugue_sqlParser.ExpressionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#expression.
    def exitExpression(self, ctx:fugue_sqlParser.ExpressionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#logicalNot.
    def enterLogicalNot(self, ctx:fugue_sqlParser.LogicalNotContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#logicalNot.
    def exitLogicalNot(self, ctx:fugue_sqlParser.LogicalNotContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#predicated.
    def enterPredicated(self, ctx:fugue_sqlParser.PredicatedContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#predicated.
    def exitPredicated(self, ctx:fugue_sqlParser.PredicatedContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#exists.
    def enterExists(self, ctx:fugue_sqlParser.ExistsContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#exists.
    def exitExists(self, ctx:fugue_sqlParser.ExistsContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#logicalBinary.
    def enterLogicalBinary(self, ctx:fugue_sqlParser.LogicalBinaryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#logicalBinary.
    def exitLogicalBinary(self, ctx:fugue_sqlParser.LogicalBinaryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#predicate.
    def enterPredicate(self, ctx:fugue_sqlParser.PredicateContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#predicate.
    def exitPredicate(self, ctx:fugue_sqlParser.PredicateContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#valueExpressionDefault.
    def enterValueExpressionDefault(self, ctx:fugue_sqlParser.ValueExpressionDefaultContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#valueExpressionDefault.
    def exitValueExpressionDefault(self, ctx:fugue_sqlParser.ValueExpressionDefaultContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#comparison.
    def enterComparison(self, ctx:fugue_sqlParser.ComparisonContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#comparison.
    def exitComparison(self, ctx:fugue_sqlParser.ComparisonContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#arithmeticBinary.
    def enterArithmeticBinary(self, ctx:fugue_sqlParser.ArithmeticBinaryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#arithmeticBinary.
    def exitArithmeticBinary(self, ctx:fugue_sqlParser.ArithmeticBinaryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#arithmeticUnary.
    def enterArithmeticUnary(self, ctx:fugue_sqlParser.ArithmeticUnaryContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#arithmeticUnary.
    def exitArithmeticUnary(self, ctx:fugue_sqlParser.ArithmeticUnaryContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#struct.
    def enterStruct(self, ctx:fugue_sqlParser.StructContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#struct.
    def exitStruct(self, ctx:fugue_sqlParser.StructContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#dereference.
    def enterDereference(self, ctx:fugue_sqlParser.DereferenceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#dereference.
    def exitDereference(self, ctx:fugue_sqlParser.DereferenceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#simpleCase.
    def enterSimpleCase(self, ctx:fugue_sqlParser.SimpleCaseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#simpleCase.
    def exitSimpleCase(self, ctx:fugue_sqlParser.SimpleCaseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#columnReference.
    def enterColumnReference(self, ctx:fugue_sqlParser.ColumnReferenceContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#columnReference.
    def exitColumnReference(self, ctx:fugue_sqlParser.ColumnReferenceContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#rowConstructor.
    def enterRowConstructor(self, ctx:fugue_sqlParser.RowConstructorContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#rowConstructor.
    def exitRowConstructor(self, ctx:fugue_sqlParser.RowConstructorContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#last.
    def enterLast(self, ctx:fugue_sqlParser.LastContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#last.
    def exitLast(self, ctx:fugue_sqlParser.LastContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#star.
    def enterStar(self, ctx:fugue_sqlParser.StarContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#star.
    def exitStar(self, ctx:fugue_sqlParser.StarContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#overlay.
    def enterOverlay(self, ctx:fugue_sqlParser.OverlayContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#overlay.
    def exitOverlay(self, ctx:fugue_sqlParser.OverlayContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#subscript.
    def enterSubscript(self, ctx:fugue_sqlParser.SubscriptContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#subscript.
    def exitSubscript(self, ctx:fugue_sqlParser.SubscriptContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#subqueryExpression.
    def enterSubqueryExpression(self, ctx:fugue_sqlParser.SubqueryExpressionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#subqueryExpression.
    def exitSubqueryExpression(self, ctx:fugue_sqlParser.SubqueryExpressionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#substring.
    def enterSubstring(self, ctx:fugue_sqlParser.SubstringContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#substring.
    def exitSubstring(self, ctx:fugue_sqlParser.SubstringContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#currentDatetime.
    def enterCurrentDatetime(self, ctx:fugue_sqlParser.CurrentDatetimeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#currentDatetime.
    def exitCurrentDatetime(self, ctx:fugue_sqlParser.CurrentDatetimeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#cast.
    def enterCast(self, ctx:fugue_sqlParser.CastContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#cast.
    def exitCast(self, ctx:fugue_sqlParser.CastContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#constantDefault.
    def enterConstantDefault(self, ctx:fugue_sqlParser.ConstantDefaultContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#constantDefault.
    def exitConstantDefault(self, ctx:fugue_sqlParser.ConstantDefaultContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#lambda.
    def enterLambda(self, ctx:fugue_sqlParser.LambdaContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#lambda.
    def exitLambda(self, ctx:fugue_sqlParser.LambdaContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#parenthesizedExpression.
    def enterParenthesizedExpression(self, ctx:fugue_sqlParser.ParenthesizedExpressionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#parenthesizedExpression.
    def exitParenthesizedExpression(self, ctx:fugue_sqlParser.ParenthesizedExpressionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#extract.
    def enterExtract(self, ctx:fugue_sqlParser.ExtractContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#extract.
    def exitExtract(self, ctx:fugue_sqlParser.ExtractContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#trim.
    def enterTrim(self, ctx:fugue_sqlParser.TrimContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#trim.
    def exitTrim(self, ctx:fugue_sqlParser.TrimContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#functionCall.
    def enterFunctionCall(self, ctx:fugue_sqlParser.FunctionCallContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#functionCall.
    def exitFunctionCall(self, ctx:fugue_sqlParser.FunctionCallContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#searchedCase.
    def enterSearchedCase(self, ctx:fugue_sqlParser.SearchedCaseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#searchedCase.
    def exitSearchedCase(self, ctx:fugue_sqlParser.SearchedCaseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#position.
    def enterPosition(self, ctx:fugue_sqlParser.PositionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#position.
    def exitPosition(self, ctx:fugue_sqlParser.PositionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#first.
    def enterFirst(self, ctx:fugue_sqlParser.FirstContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#first.
    def exitFirst(self, ctx:fugue_sqlParser.FirstContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#nullLiteral.
    def enterNullLiteral(self, ctx:fugue_sqlParser.NullLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#nullLiteral.
    def exitNullLiteral(self, ctx:fugue_sqlParser.NullLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#intervalLiteral.
    def enterIntervalLiteral(self, ctx:fugue_sqlParser.IntervalLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#intervalLiteral.
    def exitIntervalLiteral(self, ctx:fugue_sqlParser.IntervalLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#typeConstructor.
    def enterTypeConstructor(self, ctx:fugue_sqlParser.TypeConstructorContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#typeConstructor.
    def exitTypeConstructor(self, ctx:fugue_sqlParser.TypeConstructorContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#numericLiteral.
    def enterNumericLiteral(self, ctx:fugue_sqlParser.NumericLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#numericLiteral.
    def exitNumericLiteral(self, ctx:fugue_sqlParser.NumericLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#booleanLiteral.
    def enterBooleanLiteral(self, ctx:fugue_sqlParser.BooleanLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#booleanLiteral.
    def exitBooleanLiteral(self, ctx:fugue_sqlParser.BooleanLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#stringLiteral.
    def enterStringLiteral(self, ctx:fugue_sqlParser.StringLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#stringLiteral.
    def exitStringLiteral(self, ctx:fugue_sqlParser.StringLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#comparisonOperator.
    def enterComparisonOperator(self, ctx:fugue_sqlParser.ComparisonOperatorContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#comparisonOperator.
    def exitComparisonOperator(self, ctx:fugue_sqlParser.ComparisonOperatorContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#comparisonEqualOperator.
    def enterComparisonEqualOperator(self, ctx:fugue_sqlParser.ComparisonEqualOperatorContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#comparisonEqualOperator.
    def exitComparisonEqualOperator(self, ctx:fugue_sqlParser.ComparisonEqualOperatorContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#arithmeticOperator.
    def enterArithmeticOperator(self, ctx:fugue_sqlParser.ArithmeticOperatorContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#arithmeticOperator.
    def exitArithmeticOperator(self, ctx:fugue_sqlParser.ArithmeticOperatorContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#predicateOperator.
    def enterPredicateOperator(self, ctx:fugue_sqlParser.PredicateOperatorContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#predicateOperator.
    def exitPredicateOperator(self, ctx:fugue_sqlParser.PredicateOperatorContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#booleanValue.
    def enterBooleanValue(self, ctx:fugue_sqlParser.BooleanValueContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#booleanValue.
    def exitBooleanValue(self, ctx:fugue_sqlParser.BooleanValueContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#interval.
    def enterInterval(self, ctx:fugue_sqlParser.IntervalContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#interval.
    def exitInterval(self, ctx:fugue_sqlParser.IntervalContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#errorCapturingMultiUnitsInterval.
    def enterErrorCapturingMultiUnitsInterval(self, ctx:fugue_sqlParser.ErrorCapturingMultiUnitsIntervalContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#errorCapturingMultiUnitsInterval.
    def exitErrorCapturingMultiUnitsInterval(self, ctx:fugue_sqlParser.ErrorCapturingMultiUnitsIntervalContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#multiUnitsInterval.
    def enterMultiUnitsInterval(self, ctx:fugue_sqlParser.MultiUnitsIntervalContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#multiUnitsInterval.
    def exitMultiUnitsInterval(self, ctx:fugue_sqlParser.MultiUnitsIntervalContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#errorCapturingUnitToUnitInterval.
    def enterErrorCapturingUnitToUnitInterval(self, ctx:fugue_sqlParser.ErrorCapturingUnitToUnitIntervalContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#errorCapturingUnitToUnitInterval.
    def exitErrorCapturingUnitToUnitInterval(self, ctx:fugue_sqlParser.ErrorCapturingUnitToUnitIntervalContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#unitToUnitInterval.
    def enterUnitToUnitInterval(self, ctx:fugue_sqlParser.UnitToUnitIntervalContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#unitToUnitInterval.
    def exitUnitToUnitInterval(self, ctx:fugue_sqlParser.UnitToUnitIntervalContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#intervalValue.
    def enterIntervalValue(self, ctx:fugue_sqlParser.IntervalValueContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#intervalValue.
    def exitIntervalValue(self, ctx:fugue_sqlParser.IntervalValueContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#intervalUnit.
    def enterIntervalUnit(self, ctx:fugue_sqlParser.IntervalUnitContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#intervalUnit.
    def exitIntervalUnit(self, ctx:fugue_sqlParser.IntervalUnitContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#colPosition.
    def enterColPosition(self, ctx:fugue_sqlParser.ColPositionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#colPosition.
    def exitColPosition(self, ctx:fugue_sqlParser.ColPositionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#complexDataType.
    def enterComplexDataType(self, ctx:fugue_sqlParser.ComplexDataTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#complexDataType.
    def exitComplexDataType(self, ctx:fugue_sqlParser.ComplexDataTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#primitiveDataType.
    def enterPrimitiveDataType(self, ctx:fugue_sqlParser.PrimitiveDataTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#primitiveDataType.
    def exitPrimitiveDataType(self, ctx:fugue_sqlParser.PrimitiveDataTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#qualifiedColTypeWithPositionList.
    def enterQualifiedColTypeWithPositionList(self, ctx:fugue_sqlParser.QualifiedColTypeWithPositionListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#qualifiedColTypeWithPositionList.
    def exitQualifiedColTypeWithPositionList(self, ctx:fugue_sqlParser.QualifiedColTypeWithPositionListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#qualifiedColTypeWithPosition.
    def enterQualifiedColTypeWithPosition(self, ctx:fugue_sqlParser.QualifiedColTypeWithPositionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#qualifiedColTypeWithPosition.
    def exitQualifiedColTypeWithPosition(self, ctx:fugue_sqlParser.QualifiedColTypeWithPositionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#colTypeList.
    def enterColTypeList(self, ctx:fugue_sqlParser.ColTypeListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#colTypeList.
    def exitColTypeList(self, ctx:fugue_sqlParser.ColTypeListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#colType.
    def enterColType(self, ctx:fugue_sqlParser.ColTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#colType.
    def exitColType(self, ctx:fugue_sqlParser.ColTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#complexColTypeList.
    def enterComplexColTypeList(self, ctx:fugue_sqlParser.ComplexColTypeListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#complexColTypeList.
    def exitComplexColTypeList(self, ctx:fugue_sqlParser.ComplexColTypeListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#complexColType.
    def enterComplexColType(self, ctx:fugue_sqlParser.ComplexColTypeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#complexColType.
    def exitComplexColType(self, ctx:fugue_sqlParser.ComplexColTypeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#whenClause.
    def enterWhenClause(self, ctx:fugue_sqlParser.WhenClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#whenClause.
    def exitWhenClause(self, ctx:fugue_sqlParser.WhenClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#windowClause.
    def enterWindowClause(self, ctx:fugue_sqlParser.WindowClauseContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#windowClause.
    def exitWindowClause(self, ctx:fugue_sqlParser.WindowClauseContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#namedWindow.
    def enterNamedWindow(self, ctx:fugue_sqlParser.NamedWindowContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#namedWindow.
    def exitNamedWindow(self, ctx:fugue_sqlParser.NamedWindowContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#windowRef.
    def enterWindowRef(self, ctx:fugue_sqlParser.WindowRefContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#windowRef.
    def exitWindowRef(self, ctx:fugue_sqlParser.WindowRefContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#windowDef.
    def enterWindowDef(self, ctx:fugue_sqlParser.WindowDefContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#windowDef.
    def exitWindowDef(self, ctx:fugue_sqlParser.WindowDefContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#windowFrame.
    def enterWindowFrame(self, ctx:fugue_sqlParser.WindowFrameContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#windowFrame.
    def exitWindowFrame(self, ctx:fugue_sqlParser.WindowFrameContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#frameBound.
    def enterFrameBound(self, ctx:fugue_sqlParser.FrameBoundContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#frameBound.
    def exitFrameBound(self, ctx:fugue_sqlParser.FrameBoundContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#qualifiedNameList.
    def enterQualifiedNameList(self, ctx:fugue_sqlParser.QualifiedNameListContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#qualifiedNameList.
    def exitQualifiedNameList(self, ctx:fugue_sqlParser.QualifiedNameListContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#functionName.
    def enterFunctionName(self, ctx:fugue_sqlParser.FunctionNameContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#functionName.
    def exitFunctionName(self, ctx:fugue_sqlParser.FunctionNameContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#qualifiedName.
    def enterQualifiedName(self, ctx:fugue_sqlParser.QualifiedNameContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#qualifiedName.
    def exitQualifiedName(self, ctx:fugue_sqlParser.QualifiedNameContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#errorCapturingIdentifier.
    def enterErrorCapturingIdentifier(self, ctx:fugue_sqlParser.ErrorCapturingIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#errorCapturingIdentifier.
    def exitErrorCapturingIdentifier(self, ctx:fugue_sqlParser.ErrorCapturingIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#errorIdent.
    def enterErrorIdent(self, ctx:fugue_sqlParser.ErrorIdentContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#errorIdent.
    def exitErrorIdent(self, ctx:fugue_sqlParser.ErrorIdentContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#realIdent.
    def enterRealIdent(self, ctx:fugue_sqlParser.RealIdentContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#realIdent.
    def exitRealIdent(self, ctx:fugue_sqlParser.RealIdentContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#identifier.
    def enterIdentifier(self, ctx:fugue_sqlParser.IdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#identifier.
    def exitIdentifier(self, ctx:fugue_sqlParser.IdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#unquotedIdentifier.
    def enterUnquotedIdentifier(self, ctx:fugue_sqlParser.UnquotedIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#unquotedIdentifier.
    def exitUnquotedIdentifier(self, ctx:fugue_sqlParser.UnquotedIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#quotedIdentifierAlternative.
    def enterQuotedIdentifierAlternative(self, ctx:fugue_sqlParser.QuotedIdentifierAlternativeContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#quotedIdentifierAlternative.
    def exitQuotedIdentifierAlternative(self, ctx:fugue_sqlParser.QuotedIdentifierAlternativeContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#quotedIdentifier.
    def enterQuotedIdentifier(self, ctx:fugue_sqlParser.QuotedIdentifierContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#quotedIdentifier.
    def exitQuotedIdentifier(self, ctx:fugue_sqlParser.QuotedIdentifierContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#exponentLiteral.
    def enterExponentLiteral(self, ctx:fugue_sqlParser.ExponentLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#exponentLiteral.
    def exitExponentLiteral(self, ctx:fugue_sqlParser.ExponentLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#decimalLiteral.
    def enterDecimalLiteral(self, ctx:fugue_sqlParser.DecimalLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#decimalLiteral.
    def exitDecimalLiteral(self, ctx:fugue_sqlParser.DecimalLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#legacyDecimalLiteral.
    def enterLegacyDecimalLiteral(self, ctx:fugue_sqlParser.LegacyDecimalLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#legacyDecimalLiteral.
    def exitLegacyDecimalLiteral(self, ctx:fugue_sqlParser.LegacyDecimalLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#integerLiteral.
    def enterIntegerLiteral(self, ctx:fugue_sqlParser.IntegerLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#integerLiteral.
    def exitIntegerLiteral(self, ctx:fugue_sqlParser.IntegerLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#bigIntLiteral.
    def enterBigIntLiteral(self, ctx:fugue_sqlParser.BigIntLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#bigIntLiteral.
    def exitBigIntLiteral(self, ctx:fugue_sqlParser.BigIntLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#smallIntLiteral.
    def enterSmallIntLiteral(self, ctx:fugue_sqlParser.SmallIntLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#smallIntLiteral.
    def exitSmallIntLiteral(self, ctx:fugue_sqlParser.SmallIntLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#tinyIntLiteral.
    def enterTinyIntLiteral(self, ctx:fugue_sqlParser.TinyIntLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#tinyIntLiteral.
    def exitTinyIntLiteral(self, ctx:fugue_sqlParser.TinyIntLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#doubleLiteral.
    def enterDoubleLiteral(self, ctx:fugue_sqlParser.DoubleLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#doubleLiteral.
    def exitDoubleLiteral(self, ctx:fugue_sqlParser.DoubleLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#bigDecimalLiteral.
    def enterBigDecimalLiteral(self, ctx:fugue_sqlParser.BigDecimalLiteralContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#bigDecimalLiteral.
    def exitBigDecimalLiteral(self, ctx:fugue_sqlParser.BigDecimalLiteralContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#alterColumnAction.
    def enterAlterColumnAction(self, ctx:fugue_sqlParser.AlterColumnActionContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#alterColumnAction.
    def exitAlterColumnAction(self, ctx:fugue_sqlParser.AlterColumnActionContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#ansiNonReserved.
    def enterAnsiNonReserved(self, ctx:fugue_sqlParser.AnsiNonReservedContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#ansiNonReserved.
    def exitAnsiNonReserved(self, ctx:fugue_sqlParser.AnsiNonReservedContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#strictNonReserved.
    def enterStrictNonReserved(self, ctx:fugue_sqlParser.StrictNonReservedContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#strictNonReserved.
    def exitStrictNonReserved(self, ctx:fugue_sqlParser.StrictNonReservedContext):
        pass


    # Enter a parse tree produced by fugue_sqlParser#nonReserved.
    def enterNonReserved(self, ctx:fugue_sqlParser.NonReservedContext):
        pass

    # Exit a parse tree produced by fugue_sqlParser#nonReserved.
    def exitNonReserved(self, ctx:fugue_sqlParser.NonReservedContext):
        pass



del fugue_sqlParser