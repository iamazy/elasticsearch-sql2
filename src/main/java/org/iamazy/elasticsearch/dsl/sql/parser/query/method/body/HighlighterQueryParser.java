package org.iamazy.elasticsearch.dsl.sql.parser.query.method.body;

import com.google.common.collect.ImmutableList;
import org.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import org.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import org.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import org.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/2/23
 * @descrition
 **/
public class HighlighterQueryParser implements MethodQueryParser {

    private static List<String> HIGHLIGHT_METHOD = ImmutableList.of("highlight","highlighter");

    @Override
    public AtomicQuery parseMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
        return null;
    }

    @Override
    public List<String> defineMethodNames() {
        return HIGHLIGHT_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return false;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2 || invocation.getParameterCount() != 4) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }
    }
}
