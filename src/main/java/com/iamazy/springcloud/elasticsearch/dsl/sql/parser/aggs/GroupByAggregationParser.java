package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.aggs;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.iamazy.springcloud.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticDslContext;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AggregationQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.aggs.geo.GeoDistanceAggregationParser;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.aggs.search.CardinalityAggregationParser;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.aggs.search.RangeAggAggregationParser;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.aggs.search.TermsAggAggregationParser;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.aggs.search.TopHitsAggregationParser;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.sql.QueryParser;
import org.apache.commons.collections4.CollectionUtils;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
public class GroupByAggregationParser implements QueryParser {

    public static final Integer MAX_GROUP_BY_SIZE = 10000;

    public static final String AGG_BUCKET_KEY_PREFIX = "";

    private final List<GroupByMethodAggregationParser> groupByMethodAggregationParsers;

    public GroupByAggregationParser() {
        groupByMethodAggregationParsers = ImmutableList.of(
                new CardinalityAggregationParser(),
                new TermsAggAggregationParser(),
                new TopHitsAggregationParser(),
                new RangeAggAggregationParser(),
                new GeoDistanceAggregationParser()
        );
    }


    @Override
    public void parse(ElasticDslContext dslContext) {

        ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr)dslContext.getSqlObject()).getSubQuery().getQuery();
        SQLSelectGroupByClause sqlGroupBy = queryBlock.getGroupBy();
        if (sqlGroupBy != null && CollectionUtils.isNotEmpty(sqlGroupBy.getItems())) {
            String queryAs = dslContext.getParseResult().getQueryAs();
            List<AggregationBuilder> aggregationList = Lists.newArrayList();
            for (SQLExpr groupByItem : sqlGroupBy.getItems()) {
                if (!(groupByItem instanceof SQLMethodInvokeExpr)) {
                    throw new ElasticSql2DslException("[syntax error] group by item must be an agg method call");
                }
                SQLMethodInvokeExpr aggMethodExpr = (SQLMethodInvokeExpr) groupByItem;
                MethodInvocation invocation=new MethodInvocation(aggMethodExpr,queryAs,dslContext.getSqlArgs());
                GroupByMethodAggregationParser groupByMethodAggregationParser =getGroupByQueryParser(invocation);
                AggregationQuery aggregationQuery = groupByMethodAggregationParser.parseAggregationMethod(invocation);
                aggregationList.add(aggregationQuery.getAggregationBuilder());

            }
            dslContext.getParseResult().setGroupBy(aggregationList);
        }

    }


    private GroupByMethodAggregationParser getGroupByQueryParser(MethodInvocation invocation){
        for(GroupByMethodAggregationParser queryParser: groupByMethodAggregationParsers){
            if(queryParser.isMatchMethodInvocation(invocation)){
                return queryParser;
            }
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Can not support group by method query expr[%s] condition", invocation.getMethodName()));
    }
}
