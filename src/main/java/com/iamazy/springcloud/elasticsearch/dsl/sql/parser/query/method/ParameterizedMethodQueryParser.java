package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method;



import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.expr.AbstractParameterizedMethodExpression;

import java.util.Map;

public abstract class ParameterizedMethodQueryParser extends AbstractParameterizedMethodExpression implements MethodQueryParser {

    @Override
    protected abstract String defineExtraParamString(MethodInvocation invocation);

    protected abstract AtomicQuery parseMethodQueryWithExtraParams(
            MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException;

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    @Override
    public AtomicQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
        if (!isMatchMethodInvocation(invocation)) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] Expected method name is one of [%s],but get [%s]",
                            defineMethodNames(), invocation.getMethodName()));
        }
        checkMethodInvocation(invocation);

        Map<String, String> extraParamMap = generateParameterMap(invocation);
        return parseMethodQueryWithExtraParams(invocation, extraParamMap);
    }
}
