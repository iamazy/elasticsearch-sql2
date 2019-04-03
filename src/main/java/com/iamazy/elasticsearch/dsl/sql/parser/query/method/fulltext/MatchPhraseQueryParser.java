package com.iamazy.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.elasticsearch.dsl.cons.ElasticConstants;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.search.MatchQuery;

import java.util.List;
import java.util.Map;

/**
 * @author iamazy
 * @date 2019/2/20
 * @descrition
 **/
public class MatchPhraseQueryParser extends AbstractFieldSpecificMethodQueryParser {


    private static final List<String> MATCH_PHRASE_METHOD = ImmutableList.of("match_phrase", "match_phrase_query", "matchPhraseQuery");

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        String text=invocation.getParameterAsString(1);
        MatchPhraseQueryBuilder matchPhraseQueryBuilder= QueryBuilders.matchPhraseQuery(fieldName,text);
        setExtraMatchQueryParam(matchPhraseQueryBuilder,extraParams);
        return matchPhraseQueryBuilder;
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
    public List<String> defineMethodNames() {
        return MATCH_PHRASE_METHOD;
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
            throw new ElasticSql2DslException("[syntax error] Match search text can not be blank!");
        }

        if (invocation.getParameterCount() == 3) {
            String extraParamString = defineExtraParamString(invocation);
            if (StringUtils.isEmpty(extraParamString)) {
                throw new ElasticSql2DslException("[syntax error] The extra param of match method can not be blank");
            }
        }
    }

    private void setExtraMatchQueryParam(MatchPhraseQueryBuilder queryBuilder, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey(ElasticConstants.ANALYZER)) {
            String val = extraParamMap.get(ElasticConstants.ANALYZER);
            queryBuilder.analyzer(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.BOOST)) {
            String val = extraParamMap.get(ElasticConstants.BOOST);
            queryBuilder.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.SLOP)) {
            String val = extraParamMap.get(ElasticConstants.SLOP);
            queryBuilder.slop(Integer.valueOf(val));
        }

        if(extraParamMap.containsKey(ElasticConstants.ZERO_TERMS_QUERY)){
            String val=extraParamMap.get(ElasticConstants.ZERO_TERMS_QUERY).toLowerCase();
            switch (val){
                case ElasticConstants.ALL:{
                    queryBuilder.zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
                    break;
                }
                case ElasticConstants.NONE:{
                    queryBuilder.zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
                    break;
                }
                case ElasticConstants.NULL:{
                    queryBuilder.zeroTermsQuery(MatchQuery.ZeroTermsQuery.NULL);
                    break;
                }
                default:{
                    break;
                }
            }

        }
    }
}
