package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.expr;


import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;

import java.util.Map;

public interface ParameterizedMethodExpression extends MethodExpression {
    Map<String, String> generateParameterMap(MethodInvocation methodInvocation);
}
