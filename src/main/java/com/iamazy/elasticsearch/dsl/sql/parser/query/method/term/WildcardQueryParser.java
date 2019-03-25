package com.iamazy.elasticsearch.dsl.sql.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import com.iamazy.elasticsearch.dsl.sql.utils.Constants;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.WildcardQueryBuilder;


import java.util.List;
import java.util.Map;

public class WildcardQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static List<String> WILDCARD_QUERY_METHOD = ImmutableList.of("wildcard", "wildcard_query", "wildcardQuery");

    public WildcardQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public List<String> defineMethodNames() {
        return WILDCARD_QUERY_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        int extraParamIdx = 2;

        return (invocation.getParameterCount() == extraParamIdx + 1)
                ? invocation.getParameterAsString(extraParamIdx) : StringUtils.EMPTY;
    }

    @Override
    public SQLExpr defineFieldExpr(MethodInvocation invocation) {
        return invocation.getParameter(0);
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2 && invocation.getParameterCount() != 3) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String text = invocation.getParameterAsString(1);
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Wildcard search text can not be blank!");
        }

        if (invocation.getParameterCount() == 3) {
            String extraParamString = defineExtraParamString(invocation);
            if (StringUtils.isEmpty(extraParamString)) {
                throw new ElasticSql2DslException("[syntax error] The extra param of wildcard method can not be blank");
            }
        }
    }

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        String text = invocation.getParameterAsString(1);
        WildcardQueryBuilder wildcardQuery = QueryBuilders.wildcardQuery(fieldName, text);

        setExtraMatchQueryParam(wildcardQuery, extraParams);
        return wildcardQuery;
    }

    private void setExtraMatchQueryParam(WildcardQueryBuilder wildcardQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey(Constants.BOOST)) {
            String val = extraParamMap.get(Constants.BOOST);
            wildcardQuery.boost(Float.valueOf(val));
        }
        if (extraParamMap.containsKey(Constants.REWRITE)) {
            String val = extraParamMap.get(Constants.REWRITE);
            wildcardQuery.rewrite(val);
        }
    }
}
