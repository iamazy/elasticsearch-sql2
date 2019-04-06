package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.expr;


import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;

import java.util.Map;

public interface ParameterizedMethodExpression extends MethodExpression {
    Map<String, String> generateParameterMap(MethodInvocation methodInvocation);
}
