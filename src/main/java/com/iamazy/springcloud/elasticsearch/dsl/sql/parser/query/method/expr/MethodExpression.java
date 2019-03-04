package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.expr;


import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;

import java.util.List;

public interface MethodExpression {
    List<String> defineMethodNames();

    boolean isMatchMethodInvocation(MethodInvocation invocation);

    void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException;
}
