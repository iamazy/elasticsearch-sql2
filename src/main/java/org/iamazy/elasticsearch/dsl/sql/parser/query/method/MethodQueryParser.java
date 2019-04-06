package org.iamazy.elasticsearch.dsl.sql.parser.query.method;


import org.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import org.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import org.iamazy.elasticsearch.dsl.sql.parser.query.method.expr.MethodExpression;

public interface MethodQueryParser extends MethodExpression {
    AtomicQuery parseMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException;
}
