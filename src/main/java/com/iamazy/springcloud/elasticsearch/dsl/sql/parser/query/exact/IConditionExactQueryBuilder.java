package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.exact;

import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import org.elasticsearch.index.query.QueryBuilder;


@FunctionalInterface
public interface IConditionExactQueryBuilder {
    QueryBuilder buildQuery(String queryFieldName, SqlConditionOperator operator, Object[] rightParamValues);
}
