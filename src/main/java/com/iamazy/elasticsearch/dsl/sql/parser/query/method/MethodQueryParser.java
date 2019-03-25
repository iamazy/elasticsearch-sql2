package com.iamazy.elasticsearch.dsl.sql.parser.query.method;


import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.expr.MethodExpression;

public interface MethodQueryParser extends MethodExpression {
    AtomicQuery parseMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException;
}
