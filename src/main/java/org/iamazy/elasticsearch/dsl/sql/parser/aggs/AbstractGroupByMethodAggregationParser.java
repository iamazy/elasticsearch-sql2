package org.iamazy.elasticsearch.dsl.sql.parser.aggs;

import org.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import org.iamazy.elasticsearch.dsl.sql.model.AggregationQuery;
import org.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import org.iamazy.elasticsearch.dsl.sql.parser.query.method.expr.MethodExpression;

/**
 * @author iamazy
 * @date 2019/3/7
 * @descrition
 **/
public abstract class AbstractGroupByMethodAggregationParser implements MethodExpression {
    /**
     * 解析聚合函数
     *
     * @param invocation
     * @return
     * @throws ElasticSql2DslException
     */
    public abstract AggregationQuery parseAggregationMethod(MethodInvocation invocation) throws ElasticSql2DslException;

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (!isMatchMethodInvocation(invocation)) {
            throw new ElasticSql2DslException("[syntax error] Sql not support group by method:" + invocation.getMethodName());
        }
    }
}
