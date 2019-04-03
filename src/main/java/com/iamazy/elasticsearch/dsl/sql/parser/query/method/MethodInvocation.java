package com.iamazy.elasticsearch.dsl.sql.parser.query.method;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author iamazy
 */
public class MethodInvocation {
    @Getter
    private final SQLMethodInvokeExpr methodInvokeExpr;
    private final String queryAs;

    public MethodInvocation(SQLMethodInvokeExpr methodInvokeExpr, String queryAs) {
        if (methodInvokeExpr == null) {
            throw new IllegalArgumentException("method invoke expression can not be null");
        }
        this.methodInvokeExpr = methodInvokeExpr;
        this.queryAs = queryAs;
    }

    public String getQueryAs() {
        return queryAs;
    }


    public String getMethodName() {
        return methodInvokeExpr.getMethodName();
    }

    public List<SQLExpr> getParameters() {
        return methodInvokeExpr.getParameters();
    }

    public int getParameterCount() {
        return methodInvokeExpr.getParameters().size();
    }

    public SQLExpr getFirstParameter() {
        return getParameter(0);
    }

    public SQLExpr getParameter(int index) {
        return methodInvokeExpr.getParameters().get(index);
    }

    public Object getParameterAsObject(int index) {
        SQLExpr paramExpr = methodInvokeExpr.getParameters().get(index);
        return ElasticSqlArgConverter.convertSqlArg(paramExpr, false);
    }

    public String getParameterAsFormatDate(int index) {
        SQLExpr paramExpr = methodInvokeExpr.getParameters().get(index);
        return ElasticSqlArgConverter.convertSqlArg(paramExpr, true).toString();
    }

    public String getParameterAsString(int index) {
        return getParameterAsObject(index).toString();
    }

    public String getLastParameterAsString() {
        if(getParameterCount()-1>=0) {
            return getParameterAsObject(getParameterCount() - 1).toString();
        }
        return StringUtils.EMPTY;
    }

    public Double getParameterAsDouble(int index) {
        return (Double) getParameterAsObject(index);
    }

    public Long getParameterAsLong(int index) {
        return (Long) getParameterAsObject(index);
    }
}
