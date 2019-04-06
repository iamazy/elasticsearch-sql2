package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.expr;


import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;

import java.util.List;

public interface MethodExpression {

    List<String> defineMethodNames();

    boolean isMatchMethodInvocation(MethodInvocation invocation);

    void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException;
}
