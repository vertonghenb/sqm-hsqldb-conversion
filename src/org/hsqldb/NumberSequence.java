package org.hsqldb;
import java.math.BigDecimal;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.rights.Grantee;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public final class NumberSequence implements SchemaObject {
    public static final NumberSequence[] emptyArray = new NumberSequence[]{};
    private HsqlName name;
    private long currValue;
    private long lastValue;
    private boolean limitReached;
    private long    startValue;
    private long    minValue;
    private long    maxValue;
    private long    increment;
    private Type    dataType;
    private boolean isCycle;
    private boolean isAlways;
    private boolean restartValueDefault;
    public NumberSequence() {
        try {
            setDefaults(null, Type.SQL_BIGINT);
        } catch (HsqlException e) {}
    }
    public NumberSequence(HsqlName name, Type type) {
        setDefaults(name, type);
    }
    public void setDefaults(HsqlName name, Type type) {
        this.name     = name;
        this.dataType = type;
        this.name     = name;
        long min;
        long max;
        switch (dataType.typeCode) {
            case Types.TINYINT :
                max = Byte.MAX_VALUE;
                min = Byte.MIN_VALUE;
                break;
            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;
            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;
            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                if (type.scale == 0) {
                    max = Long.MAX_VALUE;
                    min = Long.MIN_VALUE;
                    break;
                }
            default :
                throw Error.error(ErrorCode.X_42563);
        }
        minValue  = min;
        maxValue  = max;
        increment = 1;
    }
    public NumberSequence(HsqlName name, long value, long increment,
                          Type type) {
        this(name, type);
        setStartValue(value);
        setIncrement(increment);
    }
    public int getType() {
        return SchemaObject.SEQUENCE;
    }
    public HsqlName getName() {
        return name;
    }
    public HsqlName getCatalogName() {
        return name.schema.schema;
    }
    public HsqlName getSchemaName() {
        return name.schema;
    }
    public Grantee getOwner() {
        return name.schema.owner;
    }
    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }
    public OrderedHashSet getComponents() {
        return null;
    }
    public void compile(Session session, SchemaObject parentObject) {}
    public String getSQL() {
        StringBuffer sb = new StringBuffer(128);
        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_SEQUENCE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName()).append(' ');
        sb.append(Tokens.T_AS).append(' ');
        sb.append(getDataType().getNameString()).append(' ');
        sb.append(Tokens.T_START).append(' ');
        sb.append(Tokens.T_WITH).append(' ');
        sb.append(startValue);
        if (getIncrement() != 1) {
            sb.append(' ').append(Tokens.T_INCREMENT).append(' ');
            sb.append(Tokens.T_BY).append(' ');
            sb.append(getIncrement());
        }
        if (!hasDefaultMinMax()) {
            sb.append(' ').append(Tokens.T_MINVALUE).append(' ');
            sb.append(getMinValue());
            sb.append(' ').append(Tokens.T_MAXVALUE).append(' ');
            sb.append(getMaxValue());
        }
        if (isCycle()) {
            sb.append(' ').append(Tokens.T_CYCLE);
        }
        if (name == null) {
            sb.append(Tokens.T_CLOSEBRACKET);
        }
        return sb.toString();
    }
    public String getSQLColumnDefinition() {
        StringBuffer sb = new StringBuffer(128);
        sb.append(Tokens.T_GENERATED).append(' ');
        if (name == null) {
            if (isAlways()) {
                sb.append(Tokens.T_ALWAYS);
            } else {
                sb.append(Tokens.T_BY).append(' ').append(Tokens.T_DEFAULT);
            }
            sb.append(' ').append(Tokens.T_AS).append(' ').append(
                Tokens.T_IDENTITY).append(Tokens.T_OPENBRACKET);
            sb.append(Tokens.T_START).append(' ');
            sb.append(Tokens.T_WITH).append(' ');
            sb.append(startValue);
            if (getIncrement() != 1) {
                sb.append(' ').append(Tokens.T_INCREMENT).append(' ');
                sb.append(Tokens.T_BY).append(' ');
                sb.append(getIncrement());
            }
            if (!hasDefaultMinMax()) {
                sb.append(' ').append(Tokens.T_MINVALUE).append(' ');
                sb.append(getMinValue());
                sb.append(' ').append(Tokens.T_MAXVALUE).append(' ');
                sb.append(getMaxValue());
            }
            if (isCycle()) {
                sb.append(' ').append(Tokens.T_CYCLE);
            }
            if (name == null) {
                sb.append(Tokens.T_CLOSEBRACKET);
            }
        } else {
            sb.append(Tokens.T_BY).append(' ').append(Tokens.T_DEFAULT);
            sb.append(' ').append(Tokens.T_AS).append(' ');
            sb.append(Tokens.T_SEQUENCE).append(' ');
            sb.append(getName().getSchemaQualifiedStatementName());
        }
        return sb.toString();
    }
    public long getChangeTimestamp() {
        return 0;
    }
    public String getRestartSQL() {
        StringBuffer sb = new StringBuffer(128);
        sb.append(Tokens.T_ALTER).append(' ');
        sb.append(Tokens.T_SEQUENCE);
        sb.append(' ').append(name.getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_RESTART);
        sb.append(' ').append(Tokens.T_WITH).append(' ').append(peek());
        return sb.toString();
    }
    public static String getRestartSQL(Table t) {
        String colname = t.getColumn(t.identityColumn).getName().statementName;
        NumberSequence seq = t.identitySequence;
        StringBuffer   sb  = new StringBuffer(128);
        sb.append(Tokens.T_ALTER).append(' ').append(Tokens.T_TABLE);
        sb.append(' ').append(t.getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_ALTER).append(' ');
        sb.append(Tokens.T_COLUMN);
        sb.append(' ').append(colname);
        sb.append(' ').append(Tokens.T_RESTART);
        sb.append(' ').append(Tokens.T_WITH).append(' ').append(seq.peek());
        return sb.toString();
    }
    public Type getDataType() {
        return dataType;
    }
    public long getIncrement() {
        return increment;
    }
    public synchronized long getStartValue() {
        return startValue;
    }
    public synchronized long getMinValue() {
        return minValue;
    }
    public synchronized long getMaxValue() {
        return maxValue;
    }
    public synchronized boolean isCycle() {
        return isCycle;
    }
    public synchronized boolean isAlways() {
        return isAlways;
    }
    public synchronized boolean hasDefaultMinMax() {
        long min;
        long max;
        switch (dataType.typeCode) {
            case Types.TINYINT :
                max = Byte.MAX_VALUE;
                min = Byte.MIN_VALUE;
                break;
            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;
            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;
            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberSequence");
        }
        return minValue == min && maxValue == max;
    }
    synchronized void setStartValue(long value) {
        if (value < minValue || value > maxValue) {
            throw Error.error(ErrorCode.X_42597);
        }
        startValue = value;
        currValue  = lastValue = startValue;
    }
    synchronized void setMinValue(long value) {
        checkInTypeRange(value);
        if (value >= maxValue || currValue < value) {
            throw Error.error(ErrorCode.X_42597);
        }
        minValue = value;
    }
    synchronized void setDefaultMinValue() {
        minValue = getDefaultMinOrMax(false);
    }
    synchronized void setMaxValue(long value) {
        checkInTypeRange(value);
        if (value <= minValue || currValue > value) {
            throw Error.error(ErrorCode.X_42597);
        }
        maxValue = value;
    }
    synchronized void setDefaultMaxValue() {
        maxValue = getDefaultMinOrMax(true);
    }
    synchronized void setIncrement(long value) {
        if (value < Short.MIN_VALUE / 2 || value > Short.MAX_VALUE / 2) {
            throw Error.error(ErrorCode.X_42597);
        }
        increment = value;
    }
    synchronized void setCurrentValueNoCheck(long value) {
        checkInTypeRange(value);
        currValue = lastValue = value;
    }
    synchronized void setStartValueNoCheck(long value) {
        checkInTypeRange(value);
        startValue = value;
        currValue  = lastValue = startValue;
    }
    synchronized void setStartValueDefault() {
        restartValueDefault = true;
    }
    synchronized void setMinValueNoCheck(long value) {
        checkInTypeRange(value);
        minValue = value;
    }
    synchronized void setMaxValueNoCheck(long value) {
        checkInTypeRange(value);
        maxValue = value;
    }
    synchronized void setCycle(boolean value) {
        isCycle = value;
    }
    synchronized void setAlways(boolean value) {
        isAlways = value;
    }
    private long getDefaultMinOrMax(boolean isMax) {
        long min;
        long max;
        switch (dataType.typeCode) {
            case Types.TINYINT :
                max = Byte.MAX_VALUE;
                min = Byte.MIN_VALUE;
                break;
            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;
            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;
            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberSequence");
        }
        return isMax ? max
                     : min;
    }
    private void checkInTypeRange(long value) {
        long min;
        long max;
        switch (dataType.typeCode) {
            case Types.TINYINT :
                max = Byte.MAX_VALUE;
                min = Byte.MIN_VALUE;
                break;
            case Types.SQL_SMALLINT :
                max = Short.MAX_VALUE;
                min = Short.MIN_VALUE;
                break;
            case Types.SQL_INTEGER :
                max = Integer.MAX_VALUE;
                min = Integer.MIN_VALUE;
                break;
            case Types.SQL_BIGINT :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                max = Long.MAX_VALUE;
                min = Long.MIN_VALUE;
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "NumberSequence");
        }
        if (value < min || value > max) {
            throw Error.error(ErrorCode.X_42597);
        }
    }
    synchronized void checkValues() {
        if (restartValueDefault) {
            currValue           = lastValue = startValue;
            restartValueDefault = false;
        }
        if (minValue >= maxValue || startValue < minValue
                || startValue > maxValue || currValue < minValue
                || currValue > maxValue) {
            throw Error.error(ErrorCode.X_42597);
        }
    }
    synchronized NumberSequence duplicate() {
        NumberSequence copy = new NumberSequence();
        copy.name       = name;
        copy.startValue = startValue;
        copy.currValue  = currValue;
        copy.lastValue  = lastValue;
        copy.increment  = increment;
        copy.dataType   = dataType;
        copy.minValue   = minValue;
        copy.maxValue   = maxValue;
        copy.isCycle    = isCycle;
        copy.isAlways   = isAlways;
        return copy;
    }
    synchronized void reset(NumberSequence other) {
        name       = other.name;
        startValue = other.startValue;
        currValue  = other.currValue;
        lastValue  = other.lastValue;
        increment  = other.increment;
        dataType   = other.dataType;
        minValue   = other.minValue;
        maxValue   = other.maxValue;
        isCycle    = other.isCycle;
        isAlways   = other.isAlways;
    }
    synchronized long userUpdate(long value) {
        if (value == currValue) {
            currValue += increment;
            return value;
        }
        if (increment > 0) {
            if (value > currValue) {
                currValue += ((value - currValue + increment) / increment)
                             * increment;
            }
        } else {
            if (value < currValue) {
                currValue += ((value - currValue + increment) / increment)
                             * increment;
            }
        }
        return value;
    }
    synchronized long systemUpdate(long value) {
        if (value == currValue) {
            currValue += increment;
            return value;
        }
        if (increment > 0) {
            if (value > currValue) {
                currValue = value + increment;
            }
        } else {
            if (value < currValue) {
                currValue = value + increment;
            }
        }
        return value;
    }
    synchronized Object getValueObject() {
        long   value = getValue();
        Object result;
        switch (dataType.typeCode) {
            default :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
                result = ValuePool.getInt((int) value);
                break;
            case Types.SQL_BIGINT :
                result = ValuePool.getLong(value);
                break;
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                result = ValuePool.getBigDecimal(new BigDecimal(value));
                break;
        }
        return result;
    }
    synchronized public long getValue() {
        if (limitReached) {
            throw Error.error(ErrorCode.X_2200H);
        }
        long nextValue;
        if (increment > 0) {
            if (currValue > maxValue - increment) {
                if (isCycle) {
                    nextValue = minValue;
                } else {
                    limitReached = true;
                    nextValue    = minValue;
                }
            } else {
                nextValue = currValue + increment;
            }
        } else {
            if (currValue < minValue - increment) {
                if (isCycle) {
                    nextValue = maxValue;
                } else {
                    limitReached = true;
                    nextValue    = minValue;
                }
            } else {
                nextValue = currValue + increment;
            }
        }
        long result = currValue;
        currValue = nextValue;
        return result;
    }
    synchronized void reset() {
        lastValue = currValue = startValue;
    }
    synchronized public long peek() {
        return currValue;
    }
    synchronized boolean resetWasUsed() {
        boolean result = lastValue != currValue;
        lastValue = currValue;
        return result;
    }
    synchronized public void reset(long value) {
        if (value < minValue || value > maxValue) {
            throw Error.error(ErrorCode.X_42597);
        }
        startValue = currValue = lastValue = value;
    }
}