package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.google.common.collect.ImmutableList;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.utils.Constants;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.ParameterizedMethodQueryParser;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;


import java.util.List;
import java.util.Map;

public class MultiMatchQueryParser extends ParameterizedMethodQueryParser {

    private static final List<String> MULTI_MATCH_METHOD = ImmutableList.of("multiMatch", "multi_match", "multi_match_query", "multiMatchQuery");

    @Override
    public List<String> defineMethodNames() {
        return MULTI_MATCH_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        int extraParamIdx = 2;

        return (invocation.getParameterCount() == extraParamIdx + 1)
                ? invocation.getParameterAsString(extraParamIdx) : StringUtils.EMPTY;
    }

    @Override
    protected AtomicQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        String[] fields = invocation.getParameterAsString(0).split(COMMA);
        String text = invocation.getParameterAsString(1);

        MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery(text, fields);
        setExtraMatchQueryParam(multiMatchQuery, extraParamMap);

        return new AtomicQuery(multiMatchQuery);
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2 && invocation.getParameterCount() != 3) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String strFields = invocation.getParameterAsString(0);
        String text = invocation.getParameterAsString(1);

        if (StringUtils.isEmpty(strFields)) {
            throw new ElasticSql2DslException("[syntax error] Search fields can not be empty!");
        }
        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Search text can not be blank!");
        }
    }

    private void setExtraMatchQueryParam(MultiMatchQueryBuilder multiMatchQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }
        if (extraParamMap.containsKey(Constants.TYPE)) {
            String val = extraParamMap.get(Constants.TYPE);
            if (Constants.BOOLEAN.equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQuery.Type.BOOLEAN);
            }
            if (Constants.PHRASE.equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQuery.Type.PHRASE);
            }
            if (Constants.PHRASE_PREFIX.equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQuery.Type.PHRASE_PREFIX);
            }
        }

        if (extraParamMap.containsKey(Constants.OPERATOR)) {
            String val = extraParamMap.get(Constants.OPERATOR);
            if (Constants.AND.equalsIgnoreCase(val)) {
                multiMatchQuery.operator(Operator.AND);
            }
            if (Constants.OR.equalsIgnoreCase(val)) {
                multiMatchQuery.operator(Operator.OR);
            }
        }

        if (extraParamMap.containsKey(Constants.MINIMUM_SHOULD_MATCH)) {
            String val = extraParamMap.get(Constants.MINIMUM_SHOULD_MATCH);
            multiMatchQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey(Constants.ANALYZER)) {
            String val = extraParamMap.get(Constants.ANALYZER);
            multiMatchQuery.analyzer(val);
        }

        if (extraParamMap.containsKey(Constants.BOOST)) {
            String val = extraParamMap.get(Constants.BOOST);
            multiMatchQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.SLOP)) {
            String val = extraParamMap.get(Constants.SLOP);
            multiMatchQuery.slop(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.PREFIX_LENGTH)) {
            String val = extraParamMap.get(Constants.PREFIX_LENGTH);
            multiMatchQuery.prefixLength(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.MAX_EXPANSIONS)) {
            String val = extraParamMap.get(Constants.MAX_EXPANSIONS);

            multiMatchQuery.maxExpansions(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.FUZZY_REWRITE)) {
            String val = extraParamMap.get(Constants.FUZZY_REWRITE);
            multiMatchQuery.fuzzyRewrite(val);
        }

        if (extraParamMap.containsKey(Constants.USE_DIS_MAX)) {
            String val = extraParamMap.get(Constants.USE_DIS_MAX);
            multiMatchQuery.useDisMax(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(Constants.TIE_BREAKER)) {
            String val = extraParamMap.get(Constants.TIE_BREAKER);
            multiMatchQuery.tieBreaker(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.ZERO_TERMS_QUERY)) {
            String val = extraParamMap.get(Constants.ZERO_TERMS_QUERY);
            if (Constants.NONE.equalsIgnoreCase(val)) {
                multiMatchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
            }
            if (Constants.ALL.equalsIgnoreCase(val)) {
                multiMatchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
            }
        }

        if (extraParamMap.containsKey(Constants.CUTOFF_FREQUENCY)) {
            String val = extraParamMap.get(Constants.CUTOFF_FREQUENCY);
            multiMatchQuery.cutoffFrequency(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.FUZZINESS)) {
            String val = extraParamMap.get(Constants.FUZZINESS).toLowerCase();

            switch (val){
                case "0":
                case "zero":{
                    multiMatchQuery.fuzziness(Fuzziness.ZERO);
                    break;
                }
                case "1":
                case "one":{
                    multiMatchQuery.fuzziness(Fuzziness.ONE);
                    break;
                }
                case "2":
                case "two":{
                    multiMatchQuery.fuzziness(Fuzziness.TWO);
                    break;
                }
                default:{
                    multiMatchQuery.fuzziness(Fuzziness.AUTO);
                    break;
                }
            }
        }
    }
}
