


package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.types.Type;


public class ExpressionValue extends Expression {

    
    ExpressionValue(Object o, Type datatype) {

        super(OpTypes.VALUE);

        nodes     = Expression.emptyArray;
        dataType  = datatype;
        valueData = o;
    }

    public byte getNullability() {
        return valueData == null ? SchemaObject.Nullability.NULLABLE
                                 : SchemaObject.Nullability.NO_NULLS;
    }

    public String getSQL() {

        switch (opType) {

            case OpTypes.VALUE :
                if (valueData == null) {
                    return Tokens.T_NULL;
                }

                return dataType.convertToSQLString(valueData);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionValue");
        }
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        switch (opType) {

            case OpTypes.VALUE :
                sb.append("VALUE = ").append(valueData);
                sb.append(", TYPE = ").append(dataType.getNameString());

                return sb.toString();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionValue");
        }
    }

    Object getValue(Session session, Type type) {

        if (dataType == type || valueData == null) {
            return valueData;
        }

        return type.convertToType(session, valueData, dataType);
    }

    public Object getValue(Session session) {
        return valueData;
    }
}
