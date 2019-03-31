package com.iamazy.elasticsearch.dsl.sql.parser.aggs.search;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import com.iamazy.elasticsearch.dsl.sql.model.AggregationQuery;
import com.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import com.iamazy.elasticsearch.dsl.sql.model.SqlArgs;
import com.iamazy.elasticsearch.dsl.sql.parser.aggs.AbstractGroupByMethodAggregationParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.elasticsearch.dsl.sql.parser.sql.QueryFieldParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
public class CardinalityAggregationParser extends AbstractGroupByMethodAggregationParser {

    private static final List<String> AGG_CARDINALITY_METHOD = ImmutableList.of("cardinality");

    @Override
    public AggregationQuery parseAggregationMethod(MethodInvocation invocation) throws ElasticSql2DslException {
        SQLExpr cardinality = invocation.getFirstParameter();
        SQLExpr precisionThreshold = null;
        if (invocation.getParameterCount() == 2) {
            precisionThreshold = invocation.getParameter(1);
        }
        AggregationBuilder cardinalityBuilder = parseCardinalityAggregation(invocation.getQueryAs(), invocation.getSqlArgs(), cardinality, precisionThreshold);
        return new AggregationQuery(cardinalityBuilder);
    }

    @Override
    public List<String> defineMethodNames() {
        return AGG_CARDINALITY_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        int paramCount = invocation.getParameterCount();
        if (paramCount != 1 && paramCount != 2) {
            return false;
        }
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }


    private AggregationBuilder parseCardinalityAggregation(String queryAs, SqlArgs args, SQLExpr cardinalityFieldExpr, SQLExpr precisionThreshold) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(cardinalityFieldExpr, queryAs);
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            if (precisionThreshold != null) {
                Number threshold = (Number) ElasticSqlArgConverter.convertSqlArg(precisionThreshold, args);
                return createCardinalityBuilder(queryField.getQueryFieldFullName(), threshold.longValue());
            }
            return createCardinalityBuilder(queryField.getQueryFieldFullName());
        } else if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            throw new ElasticSql2DslException("[syntax error] can not support aggregation defined by dollar[$]");
        } else {
            throw new ElasticSql2DslException(String.format("[syntax error] can not support cardinality aggregation for field type[%s]", queryField.getQueryFieldType()));
        }

    }

    private CardinalityAggregationBuilder createCardinalityBuilder(String fieldName, long threshold) {
        return AggregationBuilders.cardinality(fieldName + "_cardinality").field(fieldName).precisionThreshold(threshold);
    }

    private CardinalityAggregationBuilder createCardinalityBuilder(String fieldName) {
        return AggregationBuilders.cardinality(fieldName + "_cardinality").field(fieldName);
    }
}
