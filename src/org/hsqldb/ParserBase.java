package org.hsqldb;
import java.math.BigDecimal;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntKeyIntValueHashMap;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public class ParserBase {
    private Scanner scanner;
    protected Token token;
    protected boolean       isRecording;
    protected HsqlArrayList recordedStatement;
    private final Token     dummyToken = new Token();
    protected boolean isCheckOrTriggerCondition;
    protected boolean isSchemaDefinition;
    protected int     parsePosition;
    static final BigDecimal LONG_MAX_VALUE_INCREMENT =
        BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.valueOf(1));
    ParserBase(Scanner t) {
        scanner = t;
        token   = scanner.token;
    }
    public Scanner getScanner() {
        return scanner;
    }
    public int getParsePosition() {
        return parsePosition;
    }
    public void setParsePosition(int parsePosition) {
        this.parsePosition = parsePosition;
    }
    void reset(String sql) {
        scanner.reset(sql);
        parsePosition             = 0;
        isCheckOrTriggerCondition = false;
        isSchemaDefinition        = false;
        isRecording               = false;
        recordedStatement         = null;
    }
    int getPosition() {
        return scanner.getTokenPosition();
    }
    void rewind(int position) {
        if (position == scanner.getTokenPosition()) {
            return;
        }
        scanner.position(position);
        if (isRecording) {
            int i = recordedStatement.size() - 1;
            for (; i >= 0; i--) {
                Token token = (Token) recordedStatement.get(i);
                if (token.position < position) {
                    break;
                }
            }
            recordedStatement.setSize(i + 1);
        }
        read();
    }
    String getLastPart() {
        return scanner.getPart(parsePosition, scanner.getTokenPosition());
    }
    String getLastPart(int position) {
        return scanner.getPart(position, scanner.getTokenPosition());
    }
    String getLastPartAndCurrent(int position) {
        return scanner.getPart(position, scanner.getPosition());
    }
    String getStatement(int startPosition, short[] startTokens) {
        while (true) {
            if (token.tokenType == Tokens.SEMICOLON) {
                break;
            } else if (token.tokenType == Tokens.X_ENDPARSE) {
                break;
            } else {
                if (ArrayUtil.find(startTokens, token.tokenType) != -1) {
                    break;
                }
            }
            read();
        }
        String sql = scanner.getPart(startPosition,
                                     scanner.getTokenPosition());
        return sql;
    }
    String getStatementForRoutine(int startPosition, short[] startTokens) {
        int tokenIndex   = 0;;
        int semiIndex    = -1;
        int semiPosition = -1;
        while (true) {
            if (token.tokenType == Tokens.SEMICOLON) {
                semiPosition = scanner.getTokenPosition();
                semiIndex    = tokenIndex;
            } else if (token.tokenType == Tokens.X_ENDPARSE) {
                if (semiIndex > 0 && semiIndex == tokenIndex - 1) {
                    rewind(semiPosition);
                }
                break;
            } else {
                if (ArrayUtil.find(startTokens, token.tokenType) != -1) {
                    break;
                }
            }
            read();
            tokenIndex++;
        }
        String sql = scanner.getPart(startPosition,
                                     scanner.getTokenPosition());
        return sql;
    }
    void startRecording() {
        recordedStatement = new HsqlArrayList();
        recordedStatement.add(token.duplicate());
        isRecording = true;
    }
    Token getRecordedToken() {
        if (isRecording) {
            return (Token) recordedStatement.get(recordedStatement.size() - 1);
        } else {
            return dummyToken;
        }
    }
    Token[] getRecordedStatement() {
        isRecording = false;
        recordedStatement.remove(recordedStatement.size() - 1);
        Token[] tokens = new Token[recordedStatement.size()];
        recordedStatement.toArray(tokens);
        recordedStatement = null;
        return tokens;
    }
    void read() {
        scanner.scanNext();
        if (token.isMalformed) {
            int errorCode = -1;
            switch (token.tokenType) {
                case Tokens.X_MALFORMED_BINARY_STRING :
                    errorCode = ErrorCode.X_42587;
                    break;
                case Tokens.X_MALFORMED_BIT_STRING :
                    errorCode = ErrorCode.X_42588;
                    break;
                case Tokens.X_MALFORMED_UNICODE_STRING :
                    errorCode = ErrorCode.X_42586;
                    break;
                case Tokens.X_MALFORMED_STRING :
                    errorCode = ErrorCode.X_42584;
                    break;
                case Tokens.X_UNKNOWN_TOKEN :
                    errorCode = ErrorCode.X_42582;
                    break;
                case Tokens.X_MALFORMED_NUMERIC :
                    errorCode = ErrorCode.X_42585;
                    break;
                case Tokens.X_MALFORMED_COMMENT :
                    errorCode = ErrorCode.X_42589;
                    break;
                case Tokens.X_MALFORMED_IDENTIFIER :
                    errorCode = ErrorCode.X_42583;
                    break;
            }
            throw Error.error(errorCode, token.getFullString());
        }
        if (isRecording) {
            Token dup = token.duplicate();
            dup.position = scanner.getTokenPosition();
            recordedStatement.add(dup);
        }
    }
    boolean isReservedKey() {
        return scanner.token.isReservedIdentifier;
    }
    boolean isCoreReservedKey() {
        return scanner.token.isCoreReservedIdentifier;
    }
    boolean isNonReservedIdentifier() {
        return !scanner.token.isReservedIdentifier
               && (scanner.token.isUndelimitedIdentifier
                   || scanner.token.isDelimitedIdentifier);
    }
    void checkIsNonReservedIdentifier() {
        if (!isNonReservedIdentifier()) {
            throw unexpectedToken();
        }
    }
    boolean isNonCoreReservedIdentifier() {
        return !scanner.token.isCoreReservedIdentifier
               && (scanner.token.isUndelimitedIdentifier
                   || scanner.token.isDelimitedIdentifier);
    }
    void checkIsNonCoreReservedIdentifier() {
        if (!isNonCoreReservedIdentifier()) {
            throw unexpectedToken();
        }
    }
    boolean isIdentifier() {
        return scanner.token.isUndelimitedIdentifier
               || scanner.token.isDelimitedIdentifier;
    }
    void checkIsIdentifier() {
        if (!isIdentifier()) {
            throw unexpectedToken();
        }
    }
    boolean isDelimitedIdentifier() {
        return scanner.token.isDelimitedIdentifier;
    }
    void checkIsDelimitedIdentifier() {
        if (token.tokenType != Tokens.X_DELIMITED_IDENTIFIER) {
            throw Error.error(ErrorCode.X_42569);
        }
    }
    void checkIsNotQuoted() {
        if (token.tokenType == Tokens.X_DELIMITED_IDENTIFIER) {
            throw unexpectedToken();
        }
    }
    void checkIsValue() {
        if (token.tokenType != Tokens.X_VALUE) {
            throw unexpectedToken();
        }
    }
    void checkIsValue(int dataTypeCode) {
        if (token.tokenType != Tokens.X_VALUE
                || token.dataType.typeCode != dataTypeCode) {
            throw unexpectedToken();
        }
    }
    void checkIsThis(int type) {
        if (token.tokenType != type) {
            String required = Tokens.getKeyword(type);
            throw unexpectedTokenRequire(required);
        }
    }
    boolean isUndelimitedSimpleName() {
        return token.isUndelimitedIdentifier && token.namePrefix == null;
    }
    boolean isDelimitedSimpleName() {
        return token.isDelimitedIdentifier && token.namePrefix == null;
    }
    boolean isSimpleName() {
        return isNonCoreReservedIdentifier() && token.namePrefix == null;
    }
    void checkIsSimpleName() {
        if (!isSimpleName()) {
            throw unexpectedToken();
        }
    }
    void readUnquotedIdentifier(String ident) {
        checkIsSimpleName();
        if (!token.tokenString.equals(ident)) {
            throw unexpectedToken();
        }
        read();
    }
    String readQuotedString() {
        checkIsValue();
        if (token.dataType.typeCode != Types.SQL_CHAR) {
            throw Error.error(ErrorCode.X_42563);
        }
        String value = token.tokenString;
        read();
        return value;
    }
    void readThis(int tokenId) {
        if (token.tokenType != tokenId) {
            String required = Tokens.getKeyword(tokenId);
            throw unexpectedTokenRequire(required);
        }
        read();
    }
    boolean readIfThis(int tokenId) {
        if (token.tokenType == tokenId) {
            read();
            return true;
        }
        return false;
    }
    Integer readIntegerObject() {
        int value = readInteger();
        return ValuePool.getInt(value);
    }
    int readInteger() {
        boolean minus = false;
        if (token.tokenType == Tokens.MINUS) {
            minus = true;
            read();
        }
        checkIsValue();
        if (minus && token.dataType.typeCode == Types.SQL_BIGINT
                && ((Number) token.tokenValue).longValue()
                   == -(long) Integer.MIN_VALUE) {
            read();
            return Integer.MIN_VALUE;
        }
        if (token.dataType.typeCode != Types.SQL_INTEGER) {
            throw Error.error(ErrorCode.X_42563);
        }
        int val = ((Number) token.tokenValue).intValue();
        if (minus) {
            val = -val;
        }
        read();
        return val;
    }
    long readBigint() {
        boolean minus = false;
        if (token.tokenType == Tokens.MINUS) {
            minus = true;
            read();
        }
        checkIsValue();
        if (minus && token.dataType.typeCode == Types.SQL_NUMERIC
                && LONG_MAX_VALUE_INCREMENT.equals(token.tokenValue)) {
            read();
            return Long.MIN_VALUE;
        }
        if (token.dataType.typeCode != Types.SQL_INTEGER
                && token.dataType.typeCode != Types.SQL_BIGINT) {
            throw Error.error(ErrorCode.X_42563);
        }
        long val = ((Number) token.tokenValue).longValue();
        if (minus) {
            val = -val;
        }
        read();
        return val;
    }
    Expression readDateTimeIntervalLiteral() {
        int pos = getPosition();
        switch (token.tokenType) {
            case Tokens.DATE : {
                read();
                if (token.tokenType != Tokens.X_VALUE
                        || token.dataType.typeCode != Types.SQL_CHAR) {
                    break;
                }
                String s = token.tokenString;
                read();
                Object date = scanner.newDate(s);
                return new ExpressionValue(date, Type.SQL_DATE);
            }
            case Tokens.TIME : {
                read();
                if (token.tokenType != Tokens.X_VALUE
                        || token.dataType.typeCode != Types.SQL_CHAR) {
                    break;
                }
                String s = token.tokenString;
                read();
                TimeData value    = scanner.newTime(s);
                Type     dataType = scanner.dateTimeType;
                return new ExpressionValue(value, dataType);
            }
            case Tokens.TIMESTAMP : {
                read();
                if (token.tokenType != Tokens.X_VALUE
                        || token.dataType.typeCode != Types.SQL_CHAR) {
                    break;
                }
                String s = token.tokenString;
                read();
                Object date     = scanner.newTimestamp(s);
                Type   dataType = scanner.dateTimeType;
                return new ExpressionValue(date, dataType);
            }
            case Tokens.INTERVAL : {
                boolean minus = false;
                read();
                if (token.tokenType == Tokens.MINUS) {
                    read();
                    minus = true;
                } else if (token.tokenType == Tokens.PLUS) {
                    read();
                }
                if (token.tokenType != Tokens.X_VALUE
                        || token.dataType.typeCode != Types.SQL_CHAR) {
                    break;
                }
                String s = token.tokenString;
                read();
                IntervalType dataType = readIntervalType(false);
                Object       interval = scanner.newInterval(s, dataType);
                dataType = (IntervalType) scanner.dateTimeType;
                if (minus) {
                    interval = dataType.negate(interval);
                }
                return new ExpressionValue(interval, dataType);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "ParserBase");
        }
        rewind(pos);
        return null;
    }
    IntervalType readIntervalType(boolean maxPrecisionDefault) {
        int precision = maxPrecisionDefault ? IntervalType.maxIntervalPrecision
                                            : -1;
        int scale = -1;
        int startToken;
        int endToken;
        startToken = endToken = token.tokenType;
        read();
        if (token.tokenType == Tokens.OPENBRACKET) {
            read();
            precision = readInteger();
            if (precision <= 0) {
                throw Error.error(ErrorCode.X_42592);
            }
            if (token.tokenType == Tokens.COMMA) {
                if (startToken != Tokens.SECOND) {
                    throw unexpectedToken();
                }
                read();
                scale = readInteger();
                if (scale < 0) {
                    throw Error.error(ErrorCode.X_42592);
                }
            }
            readThis(Tokens.CLOSEBRACKET);
        }
        if (token.tokenType == Tokens.TO) {
            read();
            endToken = token.tokenType;
            read();
        }
        if (token.tokenType == Tokens.OPENBRACKET) {
            if (endToken != Tokens.SECOND || endToken == startToken) {
                throw unexpectedToken();
            }
            read();
            scale = readInteger();
            if (scale < 0) {
                throw Error.error(ErrorCode.X_42592);
            }
            readThis(Tokens.CLOSEBRACKET);
        }
        int startIndex = ArrayUtil.find(Tokens.SQL_INTERVAL_FIELD_CODES,
                                        startToken);
        int endIndex = ArrayUtil.find(Tokens.SQL_INTERVAL_FIELD_CODES,
                                      endToken);
        return IntervalType.getIntervalType(startIndex, endIndex, precision,
                                            scale);
    }
    static int getExpressionType(int tokenT) {
        int type = expressionTypeMap.get(tokenT, -1);
        if (type == -1) {
            throw Error.runtimeError(ErrorCode.U_S0500, "ParserBase");
        }
        return type;
    }
    private static final IntKeyIntValueHashMap expressionTypeMap =
        new IntKeyIntValueHashMap(37);
    static {
        expressionTypeMap.put(Tokens.EQUALS, OpTypes.EQUAL);
        expressionTypeMap.put(Tokens.GREATER, OpTypes.GREATER);
        expressionTypeMap.put(Tokens.LESS, OpTypes.SMALLER);
        expressionTypeMap.put(Tokens.GREATER_EQUALS, OpTypes.GREATER_EQUAL);
        expressionTypeMap.put(Tokens.LESS_EQUALS, OpTypes.SMALLER_EQUAL);
        expressionTypeMap.put(Tokens.NOT_EQUALS, OpTypes.NOT_EQUAL);
        expressionTypeMap.put(Tokens.COUNT, OpTypes.COUNT);
        expressionTypeMap.put(Tokens.MAX, OpTypes.MAX);
        expressionTypeMap.put(Tokens.MIN, OpTypes.MIN);
        expressionTypeMap.put(Tokens.SUM, OpTypes.SUM);
        expressionTypeMap.put(Tokens.AVG, OpTypes.AVG);
        expressionTypeMap.put(Tokens.EVERY, OpTypes.EVERY);
        expressionTypeMap.put(Tokens.ANY, OpTypes.SOME);
        expressionTypeMap.put(Tokens.SOME, OpTypes.SOME);
        expressionTypeMap.put(Tokens.STDDEV_POP, OpTypes.STDDEV_POP);
        expressionTypeMap.put(Tokens.STDDEV_SAMP, OpTypes.STDDEV_SAMP);
        expressionTypeMap.put(Tokens.VAR_POP, OpTypes.VAR_POP);
        expressionTypeMap.put(Tokens.VAR_SAMP, OpTypes.VAR_SAMP);
        expressionTypeMap.put(Tokens.ARRAY_AGG, OpTypes.ARRAY_AGG);
        expressionTypeMap.put(Tokens.GROUP_CONCAT, OpTypes.GROUP_CONCAT);
        expressionTypeMap.put(Tokens.MEDIAN, OpTypes.MEDIAN);
    }
    HsqlException unexpectedToken(String tokenS) {
        return Error.parseError(ErrorCode.X_42581, tokenS,
                                scanner.getLineNumber());
    }
    HsqlException unexpectedTokenRequire(String required) {
        if (token.tokenType == Tokens.X_ENDPARSE) {
            return Error.parseError(ErrorCode.X_42590,
                                    ErrorCode.TOKEN_REQUIRED,
                                    scanner.getLineNumber(), new Object[] {
                "", required
            });
        }
        String tokenS;
        if (token.charsetSchema != null) {
            tokenS = token.charsetSchema;
        } else if (token.charsetName != null) {
            tokenS = token.charsetName;
        } else if (token.namePrePrefix != null) {
            tokenS = token.namePrePrefix;
        } else if (token.namePrefix != null) {
            tokenS = token.namePrefix;
        } else {
            tokenS = token.tokenString;
        }
        return Error.parseError(ErrorCode.X_42581, ErrorCode.TOKEN_REQUIRED,
                                scanner.getLineNumber(), new Object[] {
            tokenS, required
        });
    }
    HsqlException unexpectedToken() {
        if (token.tokenType == Tokens.X_ENDPARSE) {
            return Error.parseError(ErrorCode.X_42590, null,
                                    scanner.getLineNumber());
        }
        String tokenS;
        if (token.charsetSchema != null) {
            tokenS = token.charsetSchema;
        } else if (token.charsetName != null) {
            tokenS = token.charsetName;
        } else if (token.namePrePrefix != null) {
            tokenS = token.namePrePrefix;
        } else if (token.namePrefix != null) {
            tokenS = token.namePrefix;
        } else {
            tokenS = token.tokenString;
        }
        return Error.parseError(ErrorCode.X_42581, tokenS,
                                scanner.getLineNumber());
    }
    HsqlException tooManyIdentifiers() {
        String tokenS;
        if (token.namePrePrePrefix != null) {
            tokenS = token.namePrePrePrefix;
        } else if (token.namePrePrefix != null) {
            tokenS = token.namePrePrefix;
        } else if (token.namePrefix != null) {
            tokenS = token.namePrefix;
        } else {
            tokenS = token.tokenString;
        }
        return Error.parseError(ErrorCode.X_42551, tokenS,
                                scanner.getLineNumber());
    }
    HsqlException unsupportedFeature() {
        return Error.error(ErrorCode.X_0A501, token.tokenString);
    }
    HsqlException unsupportedFeature(String string) {
        return Error.error(ErrorCode.X_0A501, string);
    }
    public Number convertToNumber(String s, NumberType type) {
        return scanner.convertToNumber(s, type);
    }
}