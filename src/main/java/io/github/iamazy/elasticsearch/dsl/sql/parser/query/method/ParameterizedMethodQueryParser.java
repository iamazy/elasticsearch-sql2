package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method;


import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import io.github.iamazy.elasticsearch.dsl.cons.CoreConstants;
import io.github.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlExprParser;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.expr.AbstractParameterizedMethodExpression;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import org.apache.commons.lang3.StringUtils;

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

        boolean highlighter = false;
        String field = null;
        for (int i = 0; i < invocation.getParameterCount(); i++) {
            SQLExpr expr = invocation.getParameter(i);
            if (StringUtils.isNotBlank(expr.toString())) {
                if (CoreConstants.HIGHLIGHTER.equalsIgnoreCase(expr.toString())) {
                    throw new ElasticSql2DslException("[syntax error] the query field can not equals to 'h#'");
                }
                if (expr.toString().startsWith(CoreConstants.HIGHLIGHTER)) {
                    field = expr.toString().substring(CoreConstants.HIGHLIGHTER.length());
                    ElasticSqlExprParser elasticSqlExprParser = new ElasticSqlExprParser(field);
                    SQLExpr sqlExpr = elasticSqlExprParser.expr();
                    invocation.getParameters().set(i, sqlExpr);
                    highlighter = true;
                }

            }
        }
        AtomicQuery atomicQuery = parseMethodQueryWithExtraParams(invocation, extraParamMap);
        if (highlighter && StringUtils.isNotBlank(field)) {
            if (atomicQuery.isNestedQuery()) {
                if (field.startsWith(CoreConstants.DOLLAR)) {
                    field = field.substring(CoreConstants.DOLLAR.length());
                }
                field = field.replace(CoreConstants.DOLLAR, CoreConstants.DOT);
                atomicQuery.getHighlighter().add(field);
            } else {
                atomicQuery.getHighlighter().add(field);
            }
        }
        return atomicQuery;
    }
}
