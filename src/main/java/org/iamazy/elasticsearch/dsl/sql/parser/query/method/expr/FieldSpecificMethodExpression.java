package org.iamazy.elasticsearch.dsl.sql.parser.query.method.expr;

import com.alibaba.druid.sql.ast.SQLExpr;
import org.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;


public interface FieldSpecificMethodExpression extends MethodExpression {
    SQLExpr defineFieldExpr(MethodInvocation invocation);
}
