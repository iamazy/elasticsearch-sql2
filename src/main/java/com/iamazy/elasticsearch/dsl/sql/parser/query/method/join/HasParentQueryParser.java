package com.iamazy.elasticsearch.dsl.sql.parser.query.method.join;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import com.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.elasticsearch.dsl.sql.model.SqlArgs;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.elasticsearch.dsl.sql.parser.sql.BoolExpressionParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.join.query.JoinQueryBuilders;

import java.util.List;

/**
 * has_parent(parentType, filterExpression)
 * <p>
 * has_parent('investment', principal > 100 and status='SUCCESS')
 *
 * @author chennan
 */
public class HasParentQueryParser implements MethodQueryParser {

    private static List<String> HAS_PARENT_METHOD = ImmutableList.of("has_parent", "hasParent", "has_parent_query", "hasParentQuery");

    @Override
    public List<String> defineMethodNames() {
        return HAS_PARENT_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }
    }

    @Override
    public AtomicQuery parseMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
        String parentType = invocation.getParameterAsString(0);
        SQLExpr filter = invocation.getParameter(1);

        BoolExpressionParser boolExpressionParser = new BoolExpressionParser();
        String queryAs = invocation.getQueryAs();
        SqlArgs sqlArgs = invocation.getSqlArgs();

        BoolQueryBuilder filterBuilder = boolExpressionParser.parseBoolQueryExpr(filter, queryAs, sqlArgs);
        HasParentQueryBuilder hasParentQueryBuilder = JoinQueryBuilders.hasParentQuery(parentType, filterBuilder, false);

        return new AtomicQuery(hasParentQueryBuilder);
    }
}
