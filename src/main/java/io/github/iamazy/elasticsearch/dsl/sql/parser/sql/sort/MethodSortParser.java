package io.github.iamazy.elasticsearch.dsl.sql.parser.sql.sort;

import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.expr.MethodExpression;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;


public interface MethodSortParser extends MethodExpression {
    SortBuilder parseMethodSortBuilder(MethodInvocation invocation, SortOrder order) throws ElasticSql2DslException;
}
