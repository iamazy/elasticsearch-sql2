package com.iamazy.elasticsearch.dsl.sql.parser.query.method.term;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.elasticsearch.dsl.sql.model.SqlArgs;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;


import java.util.List;
import java.util.function.Predicate;

public class TermLevelAtomicQueryParser {

    private final List<MethodQueryParser> methodQueryParsers;

    public TermLevelAtomicQueryParser(ParseActionListener parseActionListener) {
        methodQueryParsers = ImmutableList.of(
                new PrefixQueryParser(parseActionListener),
                new TermQueryParser(parseActionListener),
                new TermsQueryParser(parseActionListener),
                new WildcardQueryParser(parseActionListener),
                new RegexpQueryParser(parseActionListener),
                new FuzzyQueryParser(parseActionListener)
        );
    }

    public Boolean isTermLevelAtomQuery(MethodInvocation invocation) {
        return methodQueryParsers.stream().anyMatch(new Predicate<MethodQueryParser>() {
            @Override
            public boolean test(MethodQueryParser methodQueryParser) {
                return methodQueryParser.isMatchMethodInvocation(invocation);
            }
        });
    }

    public AtomicQuery parseTermLevelAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, SqlArgs sqlArgs) {
        MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs, sqlArgs);
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
