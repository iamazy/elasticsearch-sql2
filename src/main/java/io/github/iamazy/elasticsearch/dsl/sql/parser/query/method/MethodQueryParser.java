package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method;


import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.expr.MethodExpression;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;

public interface MethodQueryParser extends MethodExpression {
    AtomicQuery parseMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException;
}
