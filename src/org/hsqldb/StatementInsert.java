


package org.hsqldb;

import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.types.Type;


public class StatementInsert extends StatementDML {

    int overrideUserValue = -1;

    
    StatementInsert(Session session, Table targetTable, int[] columnMap,
                    Expression insertExpression, boolean[] checkColumns,
                    CompileContext compileContext) {

        super(StatementTypes.INSERT, StatementTypes.X_SQL_DATA_CHANGE,
              session.getCurrentSchemaHsqlName());

        this.targetTable = targetTable;
        this.baseTable   = targetTable.isTriggerInsertable() ? targetTable
                                                             : targetTable
                                                             .getBaseTable();
        this.insertColumnMap    = columnMap;
        this.insertCheckColumns = checkColumns;
        this.insertExpression   = insertExpression;

        setupChecks();
        setDatabseObjects(session, compileContext);
        checkAccessRights(session);

        isSimpleInsert = insertExpression != null
                         && insertExpression.nodes.length == 1
                         && updatableTableCheck == null;
    }

    
    StatementInsert(Session session, Table targetTable, int[] columnMap,
                    boolean[] checkColumns, QueryExpression queryExpression,
                    CompileContext compileContext, int override) {

        super(StatementTypes.INSERT, StatementTypes.X_SQL_DATA_CHANGE,
              session.getCurrentSchemaHsqlName());

        this.targetTable = targetTable;
        this.baseTable   = targetTable.isTriggerInsertable() ? targetTable
                                                             : targetTable
                                                             .getBaseTable();
        this.insertColumnMap    = columnMap;
        this.insertCheckColumns = checkColumns;
        this.queryExpression    = queryExpression;
        this.overrideUserValue  = override;

        setupChecks();
        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
    }

    
    Result getResult(Session session) {

        Result          resultOut          = null;
        RowSetNavigator generatedNavigator = null;
        PersistentStore store              = baseTable.getRowStore(session);
        int             count;

        if (generatedIndexes != null) {
            resultOut = Result.newUpdateCountResult(generatedResultMetaData,
                    0);
            generatedNavigator = resultOut.getChainedResult().getNavigator();
        }

        if (isSimpleInsert) {
            Type[] colTypes = baseTable.getColumnTypes();
            Object[] data = getInsertData(session, colTypes,
                                          insertExpression.nodes[0].nodes);

            return insertSingleRow(session, store, data);
        }

        RowSetNavigator newDataNavigator = queryExpression == null
                                           ? getInsertValuesNavigator(session)
                                           : getInsertSelectNavigator(session);

        count = newDataNavigator.getSize();

        if (count > 0) {
            insertRowSet(session, generatedNavigator, newDataNavigator);
        }

        if (baseTable.triggerLists[Trigger.INSERT_AFTER].length > 0) {
            baseTable.fireTriggers(session, Trigger.INSERT_AFTER,
                                   newDataNavigator);
        }

        if (resultOut == null) {
            resultOut = new Result(ResultConstants.UPDATECOUNT, count);
        } else {
            resultOut.setUpdateCount(count);
        }

        if (count == 0) {
            session.addWarning(HsqlException.noDataCondition);
        }

        return resultOut;
    }

    RowSetNavigator getInsertSelectNavigator(Session session) {

        Type[] colTypes  = baseTable.getColumnTypes();
        int[]  columnMap = insertColumnMap;

        
        Result                result = queryExpression.getResult(session, 0);
        RowSetNavigator       nav         = result.initialiseNavigator();
        Type[]                sourceTypes = result.metaData.columnTypes;
        RowSetNavigatorClient newData     = new RowSetNavigatorClient(2);

        while (nav.hasNext()) {
            Object[] data       = baseTable.getNewRowData(session);
            Object[] sourceData = (Object[]) nav.getNext();

            for (int i = 0; i < columnMap.length; i++) {
                int j = columnMap[i];

                if (j == this.overrideUserValue) {
                    continue;
                }

                Type sourceType = sourceTypes[i];

                data[j] = colTypes[j].convertToType(session, sourceData[i],
                                                    sourceType);
            }

            newData.add(data);
        }

        return newData;
    }

    RowSetNavigator getInsertValuesNavigator(Session session) {

        Type[] colTypes = baseTable.getColumnTypes();

        
        Expression[]          list    = insertExpression.nodes;
        RowSetNavigatorClient newData = new RowSetNavigatorClient(list.length);

        for (int j = 0; j < list.length; j++) {
            Expression[] rowArgs = list[j].nodes;
            Object[]     data    = getInsertData(session, colTypes, rowArgs);

            newData.add(data);
        }

        return newData;
    }
}
