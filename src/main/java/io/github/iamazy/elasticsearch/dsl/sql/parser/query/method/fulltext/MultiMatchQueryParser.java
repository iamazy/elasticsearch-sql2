package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import io.github.iamazy.elasticsearch.dsl.cons.ElasticConstants;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.ParameterizedMethodQueryParser;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.search.MatchQuery;


import java.util.List;
import java.util.Map;

import static io.github.iamazy.elasticsearch.dsl.cons.CoreConstants.COMMA;

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
        if (extraParamMap.containsKey(ElasticConstants.TYPE)) {
            String val = extraParamMap.get(ElasticConstants.TYPE);
            if (ElasticConstants.BOOLEAN.equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQuery.Type.BOOLEAN);
            }
            if (ElasticConstants.PHRASE.equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQuery.Type.PHRASE);
            }
            if (ElasticConstants.PHRASE_PREFIX.equalsIgnoreCase(val)) {
                multiMatchQuery.type(MatchQuery.Type.PHRASE_PREFIX);
            }
        }

        if (extraParamMap.containsKey(ElasticConstants.OPERATOR)) {
            String val = extraParamMap.get(ElasticConstants.OPERATOR);
            if (ElasticConstants.AND.equalsIgnoreCase(val)) {
                multiMatchQuery.operator(Operator.AND);
            }
            if (ElasticConstants.OR.equalsIgnoreCase(val)) {
                multiMatchQuery.operator(Operator.OR);
            }
        }

        if (extraParamMap.containsKey(ElasticConstants.MINIMUM_SHOULD_MATCH)) {
            String val = extraParamMap.get(ElasticConstants.MINIMUM_SHOULD_MATCH);
            multiMatchQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.ANALYZER)) {
            String val = extraParamMap.get(ElasticConstants.ANALYZER);
            multiMatchQuery.analyzer(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.BOOST)) {
            String val = extraParamMap.get(ElasticConstants.BOOST);
            multiMatchQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.SLOP)) {
            String val = extraParamMap.get(ElasticConstants.SLOP);
            multiMatchQuery.slop(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.PREFIX_LENGTH)) {
            String val = extraParamMap.get(ElasticConstants.PREFIX_LENGTH);
            multiMatchQuery.prefixLength(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.MAX_EXPANSIONS)) {
            String val = extraParamMap.get(ElasticConstants.MAX_EXPANSIONS);

            multiMatchQuery.maxExpansions(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.FUZZY_REWRITE)) {
            String val = extraParamMap.get(ElasticConstants.FUZZY_REWRITE);
            multiMatchQuery.fuzzyRewrite(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.TIE_BREAKER)) {
            String val = extraParamMap.get(ElasticConstants.TIE_BREAKER);
            multiMatchQuery.tieBreaker(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.ZERO_TERMS_QUERY)) {
            String val = extraParamMap.get(ElasticConstants.ZERO_TERMS_QUERY);
            if (ElasticConstants.NONE.equalsIgnoreCase(val)) {
                multiMatchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
            }
            if (ElasticConstants.ALL.equalsIgnoreCase(val)) {
                multiMatchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
            }
        }

        if (extraParamMap.containsKey(ElasticConstants.CUTOFF_FREQUENCY)) {
            String val = extraParamMap.get(ElasticConstants.CUTOFF_FREQUENCY);
            multiMatchQuery.cutoffFrequency(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.FUZZINESS)) {
            String val = extraParamMap.get(ElasticConstants.FUZZINESS).toLowerCase();

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
