package org.hsqldb;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.result.Result;
public class StatementCursor extends StatementQuery {
    public static final StatementCursor[] emptyArray = new StatementCursor[]{};
    StatementCursor(Session session, QueryExpression query,
                    CompileContext compileContext) {
        super(session, query, compileContext);
    }
    Result getResult(Session session) {
        Object[] data    = session.sessionContext.routineArguments;
        Result   current = (Result) data[data.length - 1];
        Result   head    = current;
        while (current != null) {
            if (getCursorName().name.equals(current.getMainString())) {
                current.navigator.release();
                if (head == current) {
                    head = current.getChainedResult();
                }
            }
            if (current.getChainedResult() == null) {
                break;
            }
            current = current.getChainedResult();
        }
        data[data.length - 1] = head;
        Result result = queryExpression.getResult(session, 0);
        result.setStatement(this);
        if (result.isError()) {
            return result;
        }
        result.setMainString(getCursorName().name);
        if (head == null) {
            data[data.length - 1] = result;
        } else {
            ((Result) data[data.length - 1]).addChainedResult(result);
        }
        return Result.updateZeroResult;
    }
}