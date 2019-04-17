package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.cons.ElasticConstants;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
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

        if (extraParamMap.containsKey(ElasticConstants.OPERATOR)) {
            String val = extraParamMap.get(ElasticConstants.OPERATOR).toLowerCase();
            switch (val){
                case ElasticConstants.AND:{
                    matchQuery.operator(Operator.AND);
                    break;
                }
                case ElasticConstants.OR:{
                    matchQuery.operator(Operator.OR);
                    break;
                }
                default:{ }
            }
        }

        if (extraParamMap.containsKey(ElasticConstants.MINIMUM_SHOULD_MATCH)) {
            String val = extraParamMap.get(ElasticConstants.MINIMUM_SHOULD_MATCH);
            matchQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.ANALYZER)) {
            String val = extraParamMap.get(ElasticConstants.ANALYZER);
            matchQuery.analyzer(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.BOOST)) {
            String val = extraParamMap.get(ElasticConstants.BOOST);
            matchQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.PREFIX_LENGTH)) {
            String val = extraParamMap.get(ElasticConstants.PREFIX_LENGTH);
            matchQuery.prefixLength(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.MAX_EXPANSIONS)) {
            String val = extraParamMap.get(ElasticConstants.MAX_EXPANSIONS);

            matchQuery.maxExpansions(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.FUZZY_REWRITE)) {
            String val = extraParamMap.get(ElasticConstants.FUZZY_REWRITE);
            matchQuery.fuzzyRewrite(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.FUZZY_TRANSPOSITIONS)) {
            String val = extraParamMap.get(ElasticConstants.FUZZY_TRANSPOSITIONS);
            matchQuery.fuzzyTranspositions(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.LENIENT)) {
            String val = extraParamMap.get(ElasticConstants.LENIENT);
            matchQuery.lenient(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.ZERO_TERMS_QUERY)) {
            String val = extraParamMap.get(ElasticConstants.ZERO_TERMS_QUERY).toLowerCase();
            switch (val){
                case ElasticConstants.NONE:{
                    matchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.NONE);
                    break;
                }
                case ElasticConstants.ALL:{
                    matchQuery.zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);
                    break;
                }
                default:{
                    break;
                }
            }
        }

        if (extraParamMap.containsKey(ElasticConstants.CUTOFF_FREQUENCY)) {
            String val = extraParamMap.get(ElasticConstants.CUTOFF_FREQUENCY);
            matchQuery.cutoffFrequency(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.FUZZINESS)) {
            String val = extraParamMap.get(ElasticConstants.FUZZINESS).toLowerCase();

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
