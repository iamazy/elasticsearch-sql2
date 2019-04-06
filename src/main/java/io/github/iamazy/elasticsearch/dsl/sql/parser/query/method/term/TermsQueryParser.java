package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.term;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import io.github.iamazy.elasticsearch.dsl.cons.ElasticConstants;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsQueryBuilder;

import java.util.List;
import java.util.Map;

public class TermsQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static List<String> TERMS_QUERY_METHOD = ImmutableList.of("terms", "terms_query", "termsQuery");


    @Override
    public List<String> defineMethodNames() {
        return TERMS_QUERY_METHOD;
    }

    @Override
    public SQLExpr defineFieldExpr(MethodInvocation invocation) {
        return invocation.getParameter(0);
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        String extraParamString = invocation.getLastParameterAsString();
        if (isExtraParamsString(extraParamString)) {
            return extraParamString;
        }
        return StringUtils.EMPTY;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) {
        if (invocation.getParameterCount() <= 1) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        int paramCount = invocation.getParameterCount();

        for (int idx = 1; idx < paramCount - 1; idx++) {
            String text = invocation.getParameterAsString(idx);
            if (StringUtils.isEmpty(text)) {
                throw new ElasticSql2DslException("[syntax error] Terms text can not be blank!");
            }
        }
    }

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        int paramCount = invocation.getParameterCount();

        List<String> termTextList = Lists.newArrayList();
        for (int idx = 1; idx < paramCount - 1; idx++) {
            String text = invocation.getParameterAsString(idx);
            termTextList.add(text);
        }

        String lastParamText = invocation.getLastParameterAsString();
        if (!isExtraParamsString(lastParamText)) {
            termTextList.add(lastParamText);
        }

        TermsQueryBuilder termsQuery = QueryBuilders.termsQuery(fieldName, termTextList);
        setExtraMatchQueryParam(termsQuery, extraParams);
        return termsQuery;
    }

    private void setExtraMatchQueryParam(TermsQueryBuilder termsQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey(ElasticConstants.BOOST)) {
            String val = extraParamMap.get(ElasticConstants.BOOST);
            termsQuery.boost(Float.valueOf(val));
        }
    }
}
