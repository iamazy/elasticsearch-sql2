package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.SqlArgs;

import java.util.List;

public class FullTextAtomicQueryParser {

    private final List<MethodQueryParser> methodQueryParsers;

    public FullTextAtomicQueryParser(ParseActionListener parseActionListener) {
        methodQueryParsers = ImmutableList.of(
                new MatchPhraseAtomicQueryParser(parseActionListener),
                new MatchPhrasePrefixAtomicQueryParser(parseActionListener),
                new MatchAtomicQueryParser(parseActionListener),
                new MultiMatchAtomicQueryParser(),
                new QueryStringAtomicQueryParser(),
                new SimpleQueryStringAtomicQueryParser(),
                new MatchAllAtomicQueryParser()
        );
    }

    public Boolean isFulltextAtomQuery(MethodInvocation invocation) {
        return methodQueryParsers.stream().anyMatch(methodQueryParser -> methodQueryParser.isMatchMethodInvocation(invocation));
    }

    public AtomicQuery parseFullTextAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs, SqlArgs sqlArgs) {
        MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs, sqlArgs);
        MethodQueryParser matchAtomQueryParser = getQueryParser(methodInvocation);
        return matchAtomQueryParser.parseAtomMethodQuery(methodInvocation);
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
