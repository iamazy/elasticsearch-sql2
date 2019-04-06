package org.iamazy.elasticsearch.dsl.sql.parser.query.method;



import org.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import org.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import org.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import org.iamazy.elasticsearch.dsl.sql.parser.query.method.expr.AbstractParameterizedMethodExpression;

import java.util.Map;

/**
 * @author iamazy
 */
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
    public AtomicQuery parseMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
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
