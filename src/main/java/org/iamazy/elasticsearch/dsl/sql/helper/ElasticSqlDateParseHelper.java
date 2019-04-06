package org.iamazy.elasticsearch.dsl.sql.helper;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import org.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import org.iamazy.elasticsearch.dsl.cons.CoreConstants;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
class ElasticSqlDateParseHelper {
    private static final Pattern SQL_DATE_REGEX_PATTERN_01 = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");
    private static final Pattern SQL_DATE_REGEX_PATTERN_02 = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}");
    private static final Pattern SQL_DATE_REGEX_PATTERN_03 = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");

    static boolean isDateMethod(SQLMethodInvokeExpr dateMethodExpr) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(ElasticSqlMethodInvokeHelper.DATE_METHOD, dateMethodExpr.getMethodName());
    }

    static boolean isDateArgStringValue(String date) {
        return SqlDateRegex.DATE_REGEX_01.getPattern().matcher(date).matches()
                || SqlDateRegex.DATE_REGEX_02.getPattern().matcher(date).matches()
                || SqlDateRegex.DATE_REGEX_03.getPattern().matcher(date).matches();
    }

    static boolean isDateArgObjectValue(Object date) {
        return date instanceof Date;
    }

    static String formatDefaultEsDateStringValue(String date) {
        if (SqlDateRegex.DATE_REGEX_01.getPattern().matcher(date).matches()) {
            return formatDefaultEsDate(SqlDateRegex.DATE_REGEX_01.getPatternString(), date);
        }
        if (SqlDateRegex.DATE_REGEX_02.getPattern().matcher(date).matches()) {
            return formatDefaultEsDate(SqlDateRegex.DATE_REGEX_02.getPatternString(), date);
        }
        if (SqlDateRegex.DATE_REGEX_03.getPattern().matcher(date).matches()) {
            return formatDefaultEsDate(SqlDateRegex.DATE_REGEX_03.getPatternString(), date);
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Can't support such date type: %s", date));
    }

    static String formatDefaultEsDateObjectValue(Object date) {
        if (date instanceof Date) {
            SimpleDateFormat dateFormat = new SimpleDateFormat(CoreConstants.DEFAULT_ES_DATE_FORMAT);
            return dateFormat.format(date);
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Sql cannot support such date type: %s", date.getClass()));
    }

    static String formatDefaultEsDate(String patternArg, String timeValArg) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat(patternArg);
            Date date = dateFormat.parse(timeValArg);

            dateFormat = new SimpleDateFormat(CoreConstants.DEFAULT_ES_DATE_FORMAT);
            return dateFormat.format(date);
        }
        catch (ParseException pex) {
            throw new ElasticSql2DslException("[syntax error] Parse time arg error: " + timeValArg);
        }
    }

    private enum SqlDateRegex {
        DATE_REGEX_01 {
            @Override
            Pattern getPattern() {
                return SQL_DATE_REGEX_PATTERN_01;
            }

            @Override
            String getPatternString() {
                return "yyyy-MM-dd HH:mm:ss";
            }
        },
        DATE_REGEX_02 {
            @Override
            Pattern getPattern() {
                return SQL_DATE_REGEX_PATTERN_02;
            }

            @Override
            String getPatternString() {
                return "yyyy-MM-dd HH:mm";
            }
        },
        DATE_REGEX_03 {
            @Override
            Pattern getPattern() {
                return SQL_DATE_REGEX_PATTERN_03;
            }

            @Override
            String getPatternString() {
                return "yyyy-MM-dd";
            }
        };

        abstract Pattern getPattern();

        abstract String getPatternString();
    }
}



































