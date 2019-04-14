package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.term.TermQueryParser;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.term.TermsQueryParser;


import java.util.List;

public class FullTextQueryParser {

    private final List<MethodQueryParser> methodQueryParsers;

    public FullTextQueryParser() {
        methodQueryParsers = ImmutableList.of(
                new MatchPhraseQueryParser(),
                new MatchPhrasePrefixQueryParser(),
                new MatchQueryParser(),
                new TermQueryParser(),
                new TermsQueryParser(),
                new MultiMatchQueryParser(),
                new QueryStringQueryParser(),
                new SimpleQueryStringQueryParser(),
                new MatchAllQueryParser()
        );
    }

    public Boolean isFulltextAtomQuery(MethodInvocation invocation) {
        return methodQueryParsers.stream().anyMatch(methodQueryParser -> methodQueryParser.isMatchMethodInvocation(invocation));
    }

    public AtomicQuery parseFullTextAtomQuery(SQLMethodInvokeExpr methodQueryExpr, String queryAs) {
        MethodInvocation methodInvocation = new MethodInvocation(methodQueryExpr, queryAs);
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
