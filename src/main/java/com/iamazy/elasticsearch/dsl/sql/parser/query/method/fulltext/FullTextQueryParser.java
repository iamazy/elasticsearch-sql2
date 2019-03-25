package com.iamazy.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.elasticsearch.dsl.sql.model.SqlArgs;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;

import java.util.List;

public class FullTextQueryParser {

    private final List<MethodQueryParser> methodQueryParsers;

    public FullTextQueryParser(ParseActionListener parseActionListener) {
        methodQueryParsers = ImmutableList.of(
                new MatchPhraseQueryParser(parseActionListener),
                new MatchPhrasePrefixQueryParser(parseActionListener),
                new MatchQueryParser(parseActionListener),
                new MultiMatchQueryParser(),
                new QueryStringQueryParser(),
                new SimpleQueryStringQueryParser(),
                new MatchAllQueryParser()
        );
    }

    public Boolean isFulltextAtomQuery(MethodInvocation invocation) {
        return methodQueryParsers.stream().anyMatch(methodQueryParser -> methodQueryParser.isMatchMethodInvocation(invocation));
    }

    public AtomicQuery parseFullTextAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, SqlArgs sqlArgs) {
        MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs, sqlArgs);
        MethodQueryParser matchAtomQueryParser = getQueryParser(methodInvocation);
        return matchAtomQueryParser.parseMethodQuery(methodInvocation);
    }

    private MethodQueryParser getQueryParser(MethodInvocation invocation) {
        for (MethodQueryParser methodQueryParser : methodQueryParsers) {
            if (methodQueryParser.isMatchMethodInvocation(invocation)) {
                return methodQueryParser;
            }
        }
        throw new ElasticSql2DslException(
                String.format("[syntax error] Can not support method query expr[%s] condition", invocation.getMethodName()));
    }
}
