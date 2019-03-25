package com.iamazy.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.elasticsearch.dsl.sql.utils.Constants;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;


import java.util.List;
import java.util.Map;

public class MatchQueryParser extends AbstractFieldSpecificMethodQueryParser {

    private static final List<String> MATCH_METHOD = ImmutableList.of("match", "match_query", "matchQuery");

    MatchQueryParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public List<String> defineMethodNames() {
        return MATCH_METHOD;
    }

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        String text = invocation.getParameterAsString(1);
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery(fieldName, text);
        setExtraMatchQueryParam(matchQuery, extraParams);
        return matchQuery;
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
            throw new ElasticSql2DslException("[syntax error] Match search text can not be blank!");
        }

        if (invocation.getParameterCount() == 3) {
            String extraParamString = defineExtraParamString(invocation);
            if (StringUtils.isEmpty(extraParamString)) {
                throw new ElasticSql2DslException("[syntax error] The extra param of match method can not be blank");
            }
        }
    }

    private void setExtraMatchQueryParam(MatchQueryBuilder matchQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }

        if (extraParamMap.containsKey(Constants.OPERATOR)) {
            String val = extraParamMap.get(Constants.OPERATOR).toLowerCase();
            switch (val){
                case Constants.AND:{
                    matchQuery.operator(Operator.AND);
                    break;
                }
                case Constants.OR:{
                    matchQuery.operator(Operator.OR);
                    break;
                }
                default:{ }
            }
        }

        if (extraParamMap.containsKey(Constants.MINIMUM_SHOULD_MATCH)) {
            String val = extraParamMap.get(Constants.MINIMUM_SHOULD_MATCH);
            matchQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey(Constants.ANALYZER)) {
            String val = extraParamMap.get(Constants.ANALYZER);
            matchQuery.analyzer(val);
        }

        if (extraParamMap.containsKey(Constants.BOOST)) {
            String val = extraParamMap.get(Constants.BOOST);
            matchQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.PREFIX_LENGTH)) {
            String val = extraParamMap.get(Constants.PREFIX_LENGTH);
            matchQuery.prefixLength(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.MAX_EXPANSIONS)) {
            String val = extraParamMap.get(Constants.MAX_EXPANSIONS);

            matchQuery.maxExpansions(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.FUZZY_REWRITE)) {
            String val = extraParamMap.get(Constants.FUZZY_REWRITE);
            matchQuery.fuzzyRewrite(val);
        }

        if (extraParamMap.containsKey(Constants.FUZZY_TRANSPOSITIONS)) {
            String val = extraParamMap.get(Constants.FUZZY_TRANSPOSITIONS);
            matchQuery.fuzzyTranspositions(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(Constants.LENIENT)) {
            String val = extraParamMap.get(Constants.LENIENT);
            matchQuery.setLenient(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(Constants.ZERO_TERMS_QUERY)) {
            String val = extraParamMap.get(Constants.ZERO_TERMS_QUERY).toLowerCase();
            switch (val){
                case Constants.NONE:{
                    matchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
                    break;
                }
                case Constants.ALL:{
                    matchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
                    break;
                }
                default:{
                    break;
                }
            }
        }

        if (extraParamMap.containsKey(Constants.CUTOFF_FREQUENCY)) {
            String val = extraParamMap.get(Constants.CUTOFF_FREQUENCY);
            matchQuery.cutoffFrequency(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.FUZZINESS)) {
            String val = extraParamMap.get(Constants.FUZZINESS).toLowerCase();

            switch (val){
                case "0":
                case "zero":{
                    matchQuery.fuzziness(Fuzziness.ZERO);
                    break;
                }
                case "1":
                case "one":{
                    matchQuery.fuzziness(Fuzziness.ONE);
                    break;
                }
                case "2":
                case "two":{
                    matchQuery.fuzziness(Fuzziness.TWO);
                    break;
                }
                default:{
                    matchQuery.fuzziness(Fuzziness.AUTO);
                    break;
                }
            }
        }
    }
}
