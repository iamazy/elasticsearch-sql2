package io.github.iamazy.elasticsearch.dsl.sql.parser.query.exact;

import io.github.iamazy.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * @author iamazy
 */
@FunctionalInterface
public interface IConditionExactQueryBuilder {
    QueryBuilder buildQuery(String queryFieldName, SqlConditionOperator operator, Object[] rightParamValues);
}
