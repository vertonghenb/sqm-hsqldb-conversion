package org.hsqldb;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashSet;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.DTIType;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public class SetFunction implements Serializable {
    private HashSet distinctValues;
    private boolean isDistinct;
    private int       setType;
    private int       typeCode;
    private Type      type;
    private ArrayType arrayType;
    private Type      returnType;
    private long count;
    private boolean    hasNull;
    private boolean    every = true;
    private boolean    some  = false;
    private long       currentLong;
    private double     currentDouble;
    private BigDecimal currentBigDecimal;
    private Object     currentValue;
    SetFunction(int setType, Type type, Type returnType, boolean isDistinct,
                ArrayType arrayType) {
        this.setType    = setType;
        this.type       = type;
        this.returnType = returnType;
        if (isDistinct) {
            this.isDistinct = true;
            this.arrayType  = arrayType;
            distinctValues  = new HashSet();
        }
        if (setType == OpTypes.VAR_SAMP || setType == OpTypes.STDDEV_SAMP) {
            this.sample = true;
        }
        if (type != null) {
            typeCode = type.typeCode;
            if (type.isIntervalType()) {
                typeCode = Types.SQL_INTERVAL;
            }
        }
    }
    void add(Session session, Object item) {
        if (item == null) {
            hasNull = true;
            return;
        }
        if (isDistinct && !distinctValues.add(item)) {
            return;
        }
        count++;
        switch (setType) {
            case OpTypes.COUNT :
                return;
            case OpTypes.AVG :
            case OpTypes.SUM : {
                switch (typeCode) {
                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                        currentLong += ((Number) item).intValue();
                        return;
                    case Types.SQL_INTERVAL : {
                        if (item instanceof IntervalSecondData) {
                            addLong( ( (IntervalSecondData) item).getSeconds());
                            currentLong += ( (IntervalSecondData) item).getNanos();
                            if (Math.abs(currentLong)
                                >= DTIType.nanoScaleFactors[0]) {
                                addLong(currentLong
                                        / DTIType.nanoScaleFactors[0]);
                                currentLong %= DTIType.nanoScaleFactors[0];
                            }
                        }
                        else if (item instanceof IntervalMonthData) {
                            addLong( ( (IntervalMonthData) item).units);
                        }
                        return;
                    }
                    case Types.SQL_DATE :
                    case Types.SQL_TIMESTAMP :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                            addLong( ( (TimestampData) item).getSeconds());
                            currentLong += ( (TimestampData) item).getNanos();
                            if (Math.abs(currentLong)
                                >= DTIType.nanoScaleFactors[0]) {
                                addLong(currentLong
                                        / DTIType.nanoScaleFactors[0]);
                                currentLong %= DTIType.nanoScaleFactors[0];
                            }
                            currentDouble = ( (TimestampData) item).getZone();
                        return;
                    }
                    case Types.SQL_BIGINT :
                        addLong(((Number) item).longValue());
                        return;
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        currentDouble += ((Number) item).doubleValue();
                        return;
                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        if (currentBigDecimal == null) {
                            currentBigDecimal = (BigDecimal) item;
                        } else {
                            currentBigDecimal =
                                currentBigDecimal.add((BigDecimal) item);
                        }
                        return;
                    default :
                        throw Error.error(ErrorCode.X_42563);
                }
            }
            case OpTypes.MIN : {
                if (currentValue == null) {
                    currentValue = item;
                    return;
                }
                if (type.compare(session, currentValue, item) > 0) {
                    currentValue = item;
                }
                return;
            }
            case OpTypes.MAX : {
                if (currentValue == null) {
                    currentValue = item;
                    return;
                }
                if (type.compare(session, currentValue, item) < 0) {
                    currentValue = item;
                }
                return;
            }
            case OpTypes.EVERY :
                if (!(item instanceof Boolean)) {
                    throw Error.error(ErrorCode.X_42563);
                }
                every = every && ((Boolean) item).booleanValue();
                return;
            case OpTypes.SOME :
                if (!(item instanceof Boolean)) {
                    throw Error.error(ErrorCode.X_42563);
                }
                some = some || ((Boolean) item).booleanValue();
                return;
            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                addDataPoint((Number) item);
                return;
            case OpTypes.USER_AGGREGATE :
                currentValue = item;
                return;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SetFunction");
        }
    }
    Object getValue(Session session) {
        if (hasNull) {
            session.addWarning(Error.error(ErrorCode.W_01003));
        }
        if (setType == OpTypes.COUNT) {
            if (isDistinct && type.isCharacterType()) {
                Object[] array = new Object[distinctValues.size()];
                distinctValues.toArray(array);
                SortAndSlice sort = new SortAndSlice();
                sort.prepareSingleColumn(0);
                arrayType.sort(session, array, sort);
                count = arrayType.deDuplicate(session, array, sort);
            }
            return ValuePool.getLong(count);
        }
        if (count == 0) {
            return null;
        }
        switch (setType) {
            case OpTypes.AVG : {
                switch (typeCode) {
                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                        if (returnType.scale != 0) {
                            return returnType.divide(session, currentLong,
                                                     count);
                        }
                        return new Long(currentLong / count);
                    case Types.SQL_BIGINT : {
                        long value = getLongSum().divide(
                            BigInteger.valueOf(count)).longValue();
                        return new Long(value);
                    }
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        return new Double(currentDouble / count);
                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        if (returnType.scale == type.scale) {
                            return currentBigDecimal.divide(
                                new BigDecimal(count), BigDecimal.ROUND_DOWN);
                        } else {
                            return returnType.divide(session,
                                                     currentBigDecimal, count);
                        }
                    case Types.SQL_INTERVAL : {
                        BigInteger bi =
                            getLongSum().divide(BigInteger.valueOf(count));
                        if (!NumberType.isInLongLimits(bi)) {
                            throw Error.error(ErrorCode.X_22015);
                        }
                        if (((IntervalType) type).isDaySecondIntervalType()) {
                            return new IntervalSecondData(bi.longValue(),
                                                          currentLong,
                                                          (IntervalType) type,
                                                          true);
                        } else {
                            return IntervalMonthData.newIntervalMonth(
                                bi.longValue(), (IntervalType) type);
                        }
                    }
                    case Types.SQL_DATE :
                    case Types.SQL_TIMESTAMP :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE : {
                        BigInteger bi =
                            getLongSum().divide(BigInteger.valueOf(count));
                        if (!NumberType.isInLongLimits(bi)) {
                            throw Error.error(ErrorCode.X_22015);
                        }
                        return new TimestampData(bi.longValue(), (int) currentLong, (int) currentDouble);
                    }
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "SetFunction");
                }
            }
            case OpTypes.SUM : {
                switch (typeCode) {
                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                        return new Long(currentLong);
                    case Types.SQL_BIGINT :
                        return new BigDecimal(getLongSum());
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        return new Double(currentDouble);
                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        return currentBigDecimal;
                    case Types.SQL_INTERVAL : {
                        BigInteger bi = getLongSum();
                        if (!NumberType.isInLongLimits(bi)) {
                            throw Error.error(ErrorCode.X_22015);
                        }
                        if (((IntervalType) type).isDaySecondIntervalType()) {
                            return new IntervalSecondData(bi.longValue(),
                                                          currentLong,
                                                          (IntervalType) type,
                                                          true);
                        } else {
                            return IntervalMonthData.newIntervalMonth(
                                bi.longValue(), (IntervalType) type);
                        }
                    }
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "SetFunction");
                }
            }
            case OpTypes.MIN :
            case OpTypes.MAX :
                return currentValue;
            case OpTypes.EVERY :
                return every ? Boolean.TRUE
                             : Boolean.FALSE;
            case OpTypes.SOME :
                return some ? Boolean.TRUE
                            : Boolean.FALSE;
            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
                return getStdDev();
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                return getVariance();
            case OpTypes.USER_AGGREGATE :
                return currentValue;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SetFunction");
        }
    }
    static Type getType(Session session, int setType, Type type) {
        if (setType == OpTypes.COUNT) {
            return Type.SQL_BIGINT;
        }
        int typeCode = type.isIntervalType() ? Types.SQL_INTERVAL
                                             : type.typeCode;
        switch (setType) {
            case OpTypes.AVG :
            case OpTypes.MEDIAN : {
                switch (typeCode) {
                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                    case Types.SQL_BIGINT :
                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        int scale = session.database.sqlAvgScale;
                        if (scale <= type.scale) {
                            return type;
                        }
                        int digits = ((NumberType) type).getDecimalPrecision();
                        return NumberType.getNumberType(Types.SQL_DECIMAL,
                                                        digits + scale, scale);
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                    case Types.SQL_INTERVAL :
                    case Types.SQL_DATE :
                    case Types.SQL_TIMESTAMP :
                    case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                        return type;
                    default :
                        throw Error.error(ErrorCode.X_42563);
                }
            }
            case OpTypes.SUM : {
                switch (typeCode) {
                    case Types.TINYINT :
                    case Types.SQL_SMALLINT :
                    case Types.SQL_INTEGER :
                        return Type.SQL_BIGINT;
                    case Types.SQL_BIGINT :
                        return Type.SQL_DECIMAL_BIGINT_SQR;
                    case Types.SQL_REAL :
                    case Types.SQL_FLOAT :
                    case Types.SQL_DOUBLE :
                        return Type.SQL_DOUBLE;
                    case Types.SQL_NUMERIC :
                    case Types.SQL_DECIMAL :
                        return Type.getType(type.typeCode, null, null,
                                            type.precision * 2, type.scale);
                    case Types.SQL_INTERVAL :
                        return IntervalType.newIntervalType(
                            type.typeCode, DTIType.maxIntervalPrecision,
                            type.scale);
                    default :
                        throw Error.error(ErrorCode.X_42563);
                }
            }
            case OpTypes.MIN :
            case OpTypes.MAX :
                if (type.isArrayType() || type.isLobType()) {
                    throw Error.error(ErrorCode.X_42563);
                }
                return type;
            case OpTypes.EVERY :
            case OpTypes.SOME :
                if (type.isBooleanType()) {
                    return Type.SQL_BOOLEAN;
                }
                break;
            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                if (type.isNumberType()) {
                    return Type.SQL_DOUBLE;
                }
                break;
            case OpTypes.USER_AGGREGATE :
                return type;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SetFunction");
        }
        throw Error.error(ErrorCode.X_42563);
    }
    static final BigInteger multiplier =
        BigInteger.valueOf(0x0000000100000000L);
    long hi;
    long lo;
    void addLong(long value) {
        if (value == 0) {}
        else if (value > 0) {
            hi += value >> 32;
            lo += value & 0x00000000ffffffffL;
        } else {
            if (value == Long.MIN_VALUE) {
                hi -= 0x000000080000000L;
            } else {
                long temp = ~value + 1;
                hi -= temp >> 32;
                lo -= temp & 0x00000000ffffffffL;
            }
        }
    }
    BigInteger getLongSum() {
        BigInteger biglo  = BigInteger.valueOf(lo);
        BigInteger bighi  = BigInteger.valueOf(hi);
        BigInteger result = (bighi.multiply(multiplier)).add(biglo);
        return result;
    }
    private double  sk;
    private double  vk;
    private long    n;
    private boolean initialized;
    private boolean sample;
    private void addDataPoint(Number x) {    
        double xi;
        double xsi;
        long   nm1;
        if (x == null) {
            return;
        }
        xi = x.doubleValue();
        if (!initialized) {
            n           = 1;
            sk          = xi;
            vk          = 0.0;
            initialized = true;
            return;
        }
        n++;
        nm1 = (n - 1);
        xsi = (sk - (xi * nm1));
        vk  += ((xsi * xsi) / n) / nm1;
        sk  += xi;
    }
    private Number getVariance() {
        if (!initialized) {
            return null;
        }
        return sample ? (n == 1) ? null    
                                 : new Double(vk / (double) (n - 1))
                      : new Double(vk / (double) (n));
    }
    private Number getStdDev() {
        if (!initialized) {
            return null;
        }
        return sample ? (n == 1) ? null    
                                 : new Double(Math.sqrt(vk / (double) (n - 1)))
                      : new Double(Math.sqrt(vk / (double) (n)));
    }
}