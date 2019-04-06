package io.github.iamazy.elasticsearch.dsl.sql.parser.aggs.search;

import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import io.github.iamazy.elasticsearch.dsl.sql.model.AggregationQuery;
import io.github.iamazy.elasticsearch.dsl.sql.parser.aggs.AbstractGroupByMethodAggregationParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
public class TopHitsAggregationParser extends AbstractGroupByMethodAggregationParser {

    private static final List<String> AGG_TOPHITS_METHOD = ImmutableList.of("topHits", "top_hits");

    @Override
    public AggregationQuery parseAggregationMethod(MethodInvocation invocation) throws ElasticSql2DslException {
        return new AggregationQuery(parseTopHitsAggregation(invocation.getMethodInvokeExpr()));
    }

    @Override
    public List<String> defineMethodNames() {
        return AGG_TOPHITS_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    private AggregationBuilder parseTopHitsAggregation(SQLMethodInvokeExpr topHitsFieldExpr) {
        if (topHitsFieldExpr.getParameters().size() == 1) {
            if (topHitsFieldExpr.getParameters().get(0) instanceof SQLIntegerExpr) {
                SQLIntegerExpr sizeExpr = (SQLIntegerExpr) topHitsFieldExpr.getParameters().get(0);
                return createTopHitsBuilder(sizeExpr.getNumber().intValue());
            } else {
                throw new ElasticSql2DslException("[syntax error] can not support top_hits aggregation for only one field which is not integer type");
            }
        } else if (topHitsFieldExpr.getParameters().size() == 2 && topHitsFieldExpr.getParameters().get(0) instanceof SQLCharExpr
                && topHitsFieldExpr.getParameters().get(1) instanceof SQLIntegerExpr) {
            SQLCharExpr nameExpr = (SQLCharExpr) topHitsFieldExpr.getParameters().get(0);
            SQLIntegerExpr sizeExpr = (SQLIntegerExpr) topHitsFieldExpr.getParameters().get(1);
            return createTopHitsBuilder(nameExpr.getText(), sizeExpr.getNumber().intValue());
        } else if (topHitsFieldExpr.getParameters().size() == 3 && topHitsFieldExpr.getParameters().get(0) instanceof SQLCharExpr
                && topHitsFieldExpr.getParameters().get(1) instanceof SQLIntegerExpr && topHitsFieldExpr.getParameters().get(2) instanceof SQLIntegerExpr) {
            SQLCharExpr nameExpr = (SQLCharExpr) topHitsFieldExpr.getParameters().get(0);
            SQLIntegerExpr sizeExpr = (SQLIntegerExpr) topHitsFieldExpr.getParameters().get(1);
            SQLIntegerExpr fromExpr = (SQLIntegerExpr) topHitsFieldExpr.getParameters().get(2);
            return createTopHitsBuilder(nameExpr.getText(), sizeExpr.getNumber().intValue(), fromExpr.getNumber().intValue());
        } else {
            throw new ElasticSql2DslException("[syntax error] can not support top_hits aggregation for field count bigger than 2");
        }
    }

    private TopHitsAggregationBuilder createTopHitsBuilder(int size) {
        return AggregationBuilders.topHits("agg_top_hits").size(size);
    }

    private TopHitsAggregationBuilder createTopHitsBuilder(String name, int size) {
        return AggregationBuilders.topHits(name).size(size);
    }

    private TopHitsAggregationBuilder createTopHitsBuilder(String name, int size, int from) {
        return AggregationBuilders.topHits(name).size(size).from(from);
    }
}
