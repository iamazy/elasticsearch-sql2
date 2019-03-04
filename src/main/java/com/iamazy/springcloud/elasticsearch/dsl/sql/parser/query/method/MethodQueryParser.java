package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method;


import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.expr.MethodExpression;

public interface MethodQueryParser extends MethodExpression {
    AtomicQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException;
}
