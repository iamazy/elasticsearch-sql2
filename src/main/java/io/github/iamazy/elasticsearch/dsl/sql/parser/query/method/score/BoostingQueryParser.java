package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.score;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLNumberExpr;
import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;
import io.github.iamazy.elasticsearch.dsl.sql.parser.sql.BoolExpressionParser;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/4/9
 * @descrition
 **/
public class BoostingQueryParser implements MethodQueryParser {

    private static List<String> BOOSTING_METHOD = ImmutableList.of("boosting");

    @Override
    public AtomicQuery parseMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {

        SQLExpr positiveExpr = invocation.getParameter(0);
        SQLExpr negativeExpr=invocation.getParameter(1);
        Float negativeBoost=((SQLNumberExpr) invocation.getParameter(2)).getNumber().floatValue();
        BoolExpressionParser boolExpressionParser = new BoolExpressionParser();
        BoolQueryBuilder positiveQuery = boolExpressionParser.parseBoolQueryExpr(positiveExpr, invocation.getQueryAs());
        BoolQueryBuilder negativeQuery=boolExpressionParser.parseBoolQueryExpr(negativeExpr,invocation.getQueryAs());
        BoostingQueryBuilder boostingQueryBuilder = QueryBuilders.boostingQuery(positiveQuery,negativeQuery).negativeBoost(negativeBoost);
        AtomicQuery atomicQuery= new AtomicQuery(boostingQueryBuilder);
        atomicQuery.getHighlighter().addAll(boolExpressionParser.getHighlighter());
        return atomicQuery;

    }

    @Override
    public List<String> defineMethodNames() {
        return BOOSTING_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 3) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }
    }
}
