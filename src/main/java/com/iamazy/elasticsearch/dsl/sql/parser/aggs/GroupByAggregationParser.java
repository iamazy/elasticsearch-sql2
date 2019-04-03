package com.iamazy.elasticsearch.dsl.sql.parser.aggs;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.elasticsearch.dsl.sql.model.AggregationQuery;
import com.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.join.NestedAggregationParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.elasticsearch.dsl.sql.parser.sql.QueryParser;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.geo.GeoDistanceAggregationParser;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.search.CardinalityAggregationParser;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.search.RangeAggAggregationParser;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.search.TermsAggAggregationParser;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.search.TopHitsAggregationParser;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
public class GroupByAggregationParser implements QueryParser {

    public static final Integer MAX_GROUP_BY_SIZE = 10000;

    public static final String AGG_BUCKET_KEY_PREFIX = "";

    private final List<AbstractGroupByMethodAggregationParser> abstractGroupByMethodAggregationParsers;

    public GroupByAggregationParser() {
        abstractGroupByMethodAggregationParsers = ImmutableList.of(
                new CardinalityAggregationParser(),
                new TermsAggAggregationParser(),
                new TopHitsAggregationParser(),
                new RangeAggAggregationParser(),
                new GeoDistanceAggregationParser(),
                new NestedAggregationParser()
        );
    }


    @Override
    public void parse(ElasticDslContext dslContext) {

        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr) dslContext.getSqlObject()).getSubQuery().getQuery();
        SQLSelectGroupByClause sqlGroupBy = queryBlock.getGroupBy();
        if (sqlGroupBy != null && CollectionUtils.isNotEmpty(sqlGroupBy.getItems())) {
            String queryAs = dslContext.getParseResult().getQueryAs();
            List<AggregationBuilder> aggregationList = Lists.newArrayList();
            for (SQLExpr groupByItem : sqlGroupBy.getItems()) {
                aggregationList.add((AggregationBuilder) recursiveParseGroupByItemExpr(groupByItem, queryAs));
            }
            dslContext.getParseResult().setGroupBy(aggregationList);
        }

    }

    private Object recursiveParseGroupByItemExpr(SQLExpr sqlExpr, String queryAs) {
        if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr binOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLBinaryOperator binOperator = binOpExpr.getOperator();
            if (SQLBinaryOperator.GreaterThan == binOperator) {
                SQLExpr left = binOpExpr.getLeft();
                SQLExpr right = binOpExpr.getRight();
                Object leftObject = recursiveParseGroupByItemExpr(left, null);
                Object rightObject = recursiveParseGroupByItemExpr(right, null);
                AggregationBuilder leftAggBuilder, rightAggBuilder = null;
                List<AggregationBuilder> rightAggBuilders = null;
                if (leftObject instanceof AggregationBuilder) {
                    leftAggBuilder = (AggregationBuilder) recursiveParseGroupByItemExpr(left, null);
                } else {
                    throw new ElasticSql2DslException("[syntax error] left group by item only support an agg method call");
                }

                if (rightObject instanceof AggregationBuilder) {
                    rightAggBuilder = (AggregationBuilder) recursiveParseGroupByItemExpr(right, null);
                } else if (rightObject instanceof List) {
                    rightAggBuilders = (List<AggregationBuilder>) recursiveParseGroupByItemExpr(right, null);
                }

                if (rightAggBuilder != null) {
                    return leftAggBuilder.subAggregation(rightAggBuilder);
                } else if (rightAggBuilders != null) {
                    for (AggregationBuilder aggBuilder : rightAggBuilders) {
                        leftAggBuilder.subAggregation(aggBuilder);
                    }
                    return leftAggBuilder;
                } else {
                    throw new ElasticSql2DslException("[syntax error] group by item only support gt(>) operator or list operator or an agg method call");
                }
            } else {
                throw new ElasticSql2DslException("[syntax error] group by item only support gt(>) operator or list operator or an agg method call");
            }
        } else if (sqlExpr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr aggMethodExpr = (SQLMethodInvokeExpr) sqlExpr;
            MethodInvocation invocation = new MethodInvocation(aggMethodExpr, queryAs);
            AbstractGroupByMethodAggregationParser abstractGroupByMethodAggregationParser = getGroupByQueryParser(invocation);
            AggregationQuery aggregationQuery = abstractGroupByMethodAggregationParser.parseAggregationMethod(invocation);
            return aggregationQuery.getAggregationBuilder();
        } else if (sqlExpr instanceof SQLListExpr) {
            SQLListExpr sqlListExpr = (SQLListExpr) sqlExpr;
            Object[] objects = ElasticSqlArgConverter.convertSqlArgs(sqlListExpr.getItems());
            List<AggregationBuilder> aggList = new ArrayList<>(0);
            for (Object object : objects) {
                SQLExpr expr = (SQLExpr) object;
                AggregationBuilder aggregationBuilder = (AggregationBuilder) recursiveParseGroupByItemExpr(expr, queryAs);
                aggList.add(aggregationBuilder);
            }
            return aggList;
        } else {
            throw new ElasticSql2DslException("[syntax error] inside group by item only support list operator or method call");
        }
    }


    private AbstractGroupByMethodAggregationParser getGroupByQueryParser(MethodInvocation invocation) {
        for (AbstractGroupByMethodAggregationParser queryParser : abstractGroupByMethodAggregationParsers) {
            if (queryParser.isMatchMethodInvocation(invocation)) {
                return queryParser;
            }
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Can not support group by method query expr[%s] condition", invocation.getMethodName()));
    }
}
