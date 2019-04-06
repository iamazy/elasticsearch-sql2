package io.github.iamazy.elasticsearch.dsl.sql.parser.aggs.search;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import io.github.iamazy.elasticsearch.dsl.sql.model.AggregationQuery;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import io.github.iamazy.elasticsearch.dsl.sql.parser.aggs.AbstractGroupByMethodAggregationParser;
import io.github.iamazy.elasticsearch.dsl.sql.parser.aggs.GroupByAggregationParser;
import io.github.iamazy.elasticsearch.dsl.sql.parser.sql.QueryFieldParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
public class TermsAggAggregationParser extends AbstractGroupByMethodAggregationParser {

    private static final List<String> AGG_TERMS_METHOD = ImmutableList.of("terms", "terms_agg");

    @Override
    public AggregationQuery parseAggregationMethod(MethodInvocation invocation) throws ElasticSql2DslException {
        SQLExpr termsFieldExpr = invocation.getFirstParameter();
        SQLExpr shardSizeExpr = null;
        if (invocation.getParameterCount() == 2) {
            shardSizeExpr = invocation.getParameters().get(1);
        }
        AggregationBuilder termsBuilder = parseTermsAggregation(invocation.getQueryAs(), termsFieldExpr, shardSizeExpr);
        return new AggregationQuery(termsBuilder);
    }

    @Override
    public List<String> defineMethodNames() {
        return AGG_TERMS_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }

    private AggregationBuilder parseTermsAggregation(String queryAs, SQLExpr termsFieldExpr, SQLExpr shardSizeExpr) {
        QueryFieldParser queryFieldParser = new QueryFieldParser();
        ElasticSqlQueryField queryField = queryFieldParser.parseConditionQueryField(termsFieldExpr, queryAs);
        if (queryField.getQueryFieldType() == QueryFieldType.RootDocField || queryField.getQueryFieldType() == QueryFieldType.InnerDocField) {
            if (shardSizeExpr != null) {
                Number termBuckets = (Number) ElasticSqlArgConverter.convertSqlArg(shardSizeExpr);
                return createTermsBuilder(queryField.getQueryFieldFullName(), termBuckets.intValue());
            }
            return createTermsBuilder(queryField.getQueryFieldFullName());
        } else if (queryField.getQueryFieldType() == QueryFieldType.NestedDocField) {
            throw new ElasticSql2DslException("[syntax error] can not support aggregation defined by dollar[$]");
        } else {
            throw new ElasticSql2DslException(String.format("[syntax error] can not support terms aggregation for field type[%s]", queryField.getQueryFieldType()));
        }
    }

    private TermsAggregationBuilder createTermsBuilder(String termsFieldName, int termBuckets) {
        return AggregationBuilders.terms(GroupByAggregationParser.AGG_BUCKET_KEY_PREFIX + termsFieldName + "_terms")
                .field(termsFieldName)
                .minDocCount(1).shardMinDocCount(1)
                .shardSize(termBuckets << 1).size(termBuckets).order(BucketOrder.count(false));
    }

    private TermsAggregationBuilder createTermsBuilder(String termsFieldName) {
        return createTermsBuilder(termsFieldName, GroupByAggregationParser.MAX_GROUP_BY_SIZE);
    }

}
