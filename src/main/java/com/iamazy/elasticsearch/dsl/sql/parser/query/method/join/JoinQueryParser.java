package com.iamazy.elasticsearch.dsl.sql.parser.query.method.join;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.elasticsearch.dsl.sql.model.SqlArgs;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;

import java.util.List;

public class JoinQueryParser {

    private final List<MethodQueryParser> joinQueryParsers;

    public JoinQueryParser() {
        joinQueryParsers = ImmutableList.of(
                new HasParentQueryParser(),
                new HasChildQueryParser()
        );
    }

    public Boolean isJoinAtomQuery(MethodInvocation invocation) {
        return joinQueryParsers.stream().anyMatch(methodQueryParser -> methodQueryParser.isMatchMethodInvocation(invocation));
    }

    public AtomicQuery parseJoinAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, SqlArgs sqlArgs) {
        MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs, sqlArgs);
        MethodQueryParser joinAtomQueryParser = getQueryParser(methodInvocation);
        return joinAtomQueryParser.parseMethodQuery(methodInvocation);
    }

    private MethodQueryParser getQueryParser(MethodInvocation methodInvocation) {
        for (MethodQueryParser joinQueryParserItem : joinQueryParsers) {
            if (joinQueryParserItem.isMatchMethodInvocation(methodInvocation)) {
                return joinQueryParserItem;
            }
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Can not support join query expr[%s] condition",
                        methodInvocation.getMethodName()));
    }
}
