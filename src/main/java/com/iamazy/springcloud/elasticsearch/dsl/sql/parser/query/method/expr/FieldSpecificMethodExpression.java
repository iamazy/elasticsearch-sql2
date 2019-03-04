package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.expr;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;


public interface FieldSpecificMethodExpression extends MethodExpression {
    SQLExpr defineFieldExpr(MethodInvocation invocation);
}
