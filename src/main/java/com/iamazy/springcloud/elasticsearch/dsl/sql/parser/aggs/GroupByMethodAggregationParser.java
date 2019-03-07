package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.aggs;

import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.expr.MethodExpression;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AggregationQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;

/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
public interface GroupByMethodAggregationParser extends MethodExpression {
    /**
     * 解析聚合函数
     * @param invocation
     * @return
     * @throws ElasticSql2DslException
     */
    AggregationQuery parseAggregationMethod(MethodInvocation invocation) throws ElasticSql2DslException;

    @Override
    default void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException{
        if (!isMatchMethodInvocation(invocation)) {
            throw new ElasticSql2DslException("[syntax error] Sql not support group by method:" + invocation.getMethodName());
        }
    }
}
