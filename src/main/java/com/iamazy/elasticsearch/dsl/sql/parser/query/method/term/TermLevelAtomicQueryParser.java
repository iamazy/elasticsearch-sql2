package com.iamazy.elasticsearch.dsl.sql.parser.query.method.term;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;


import java.util.List;

public class TermLevelAtomicQueryParser {

    private final List<MethodQueryParser> methodQueryParsers;

    public TermLevelAtomicQueryParser() {
        methodQueryParsers = ImmutableList.of(
                new PrefixQueryParser(),
                new TermQueryParser(),
                new TermsQueryParser(),
                new WildcardQueryParser(),
                new RegexpQueryParser(),
                new FuzzyQueryParser()
        );
    }

    public Boolean isTermLevelAtomQuery(MethodInvocation invocation) {
        return methodQueryParsers.stream().anyMatch(methodQueryParser -> methodQueryParser.isMatchMethodInvocation(invocation));
    }

    public AtomicQuery parseTermLevelAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs) {
        MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs);
        MethodQueryParser matchAtomQueryParser = getQueryParser(methodInvocation);
        return matchAtomQueryParser.parseMethodQuery(methodInvocation);
    }

    private MethodQueryParser getQueryParser(MethodInvocation methodInvocation) {
        for (MethodQueryParser methodQueryParserItem : methodQueryParsers) {
            if (methodQueryParserItem.isMatchMethodInvocation(methodInvocation)) {
                return methodQueryParserItem;
            }
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Can not support method query expr[%s] condition",
                        methodInvocation.getMethodName()));
    }
}
