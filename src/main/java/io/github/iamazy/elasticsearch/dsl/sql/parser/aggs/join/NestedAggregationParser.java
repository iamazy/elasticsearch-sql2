package io.github.iamazy.elasticsearch.dsl.sql.parser.aggs.join;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlMethodInvokeHelper;
import io.github.iamazy.elasticsearch.dsl.sql.model.AggregationQuery;
import io.github.iamazy.elasticsearch.dsl.sql.model.ElasticSqlQueryField;
import io.github.iamazy.elasticsearch.dsl.sql.parser.aggs.AbstractGroupByMethodAggregationParser;
import io.github.iamazy.elasticsearch.dsl.sql.parser.sql.QueryFieldParser;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;

import java.util.List;

public class NestedAggregationParser extends AbstractGroupByMethodAggregationParser {


    private static final List<String> AGG_NESTED_METHOD = ImmutableList.of("nested","nested_agg");


    @Override
    public AggregationQuery parseAggregationMethod(MethodInvocation invocation) throws ElasticSql2DslException {
        SQLExpr nested=invocation.getFirstParameter();
        AggregationBuilder nestedBuilder=parseNestedAggregation(invocation.getQueryAs(),nested);
        return new AggregationQuery(nestedBuilder);
    }

    @Override
    public List<String> defineMethodNames() {
        return AGG_NESTED_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        int paramCount=invocation.getParameterCount();
        if(paramCount!=1){
            return false;
        }
        return ElasticSqlMethodInvokeHelper.isMethodOf(defineMethodNames(), invocation.getMethodName());
    }


    private AggregationBuilder parseNestedAggregation(String queryAs, SQLExpr nestedExpr){
        QueryFieldParser queryFieldParser=new QueryFieldParser();
        ElasticSqlQueryField elasticSqlQueryField = queryFieldParser.parseConditionQueryField(nestedExpr, queryAs);
        if(elasticSqlQueryField.getQueryFieldType()== QueryFieldType.RootDocField||elasticSqlQueryField.getQueryFieldType()==QueryFieldType.InnerDocField){
            return createNestedBuilder(elasticSqlQueryField.getQueryFieldFullName());
        }else if(elasticSqlQueryField.getQueryFieldType()==QueryFieldType.NestedDocField){
            throw new ElasticSql2DslException("[syntax error] can not support aggregation defined by dollar[$]");
        }
        else {
            throw new ElasticSql2DslException(String.format("[syntax error] can not support nested aggregation for field type[%s]", elasticSqlQueryField.getQueryFieldType()));
        }
    }


    private NestedAggregationBuilder createNestedBuilder(String fieldName) {
        return AggregationBuilders.nested(fieldName + "_nested",fieldName);
    }
}
