package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.google.common.collect.ImmutableList;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.springcloud.elasticsearch.dsl.sql.utils.Constants;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.ParameterizedMethodQueryParser;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.util.List;
import java.util.Map;

public class QueryStringAtomicQueryParser extends ParameterizedMethodQueryParser {

    private static List<String> QUERY_STRING_METHOD = ImmutableList.of("queryString", "query_string");

    @Override
    public List<String> defineMethodNames() {
        return QUERY_STRING_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        return isExtraParamsString(invocation.getLastParameterAsString())
                ? invocation.getLastParameterAsString() : StringUtils.EMPTY;
    }

    @Override
    protected AtomicQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        String text = invocation.getParameterAsString(0);
        QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryStringQuery(text);

        String queryFields ;
        if (invocation.getParameterCount() == 3) {
            queryFields = invocation.getParameterAsString(1);

            if (StringUtils.isNotBlank(queryFields)) {
                String[] tArr = queryFields.split(COLON);
                if (Constants.FIELDS.equalsIgnoreCase(tArr[0])) {
                    for (String fieldItem : tArr[1].split(COMMA)) {
                        queryStringQuery.field(fieldItem);
                    }
                }

                if (Constants.DEFAULT_FIELD.equalsIgnoreCase(tArr[0])) {
                    queryStringQuery.defaultField(tArr[1]);
                }
            }
        }

        setExtraMatchQueryParam(queryStringQuery, extraParamMap);
        return new AtomicQuery(queryStringQuery);
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        int paramCount = invocation.getParameterCount();
        if (paramCount != 1 && paramCount != 2 && paramCount != 3) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String text = invocation.getParameterAsString(0);

        if (StringUtils.isEmpty(text)) {
            throw new ElasticSql2DslException("[syntax error] Search text can not be blank!");
        }

        if (paramCount == 3) {
            String strFields = invocation.getParameterAsString(1);

            if (StringUtils.isEmpty(text)) {
                throw new ElasticSql2DslException("[syntax error] Search fields can not be empty!");
            }
            String[] tArr = strFields.split(COLON);

            if (tArr.length != 2) {
                throw new ElasticSql2DslException("[syntax error] queryString method args error");
            }

            if (Boolean.FALSE == Constants.FIELDS.equalsIgnoreCase(tArr[0]) && Boolean.FALSE == Constants.DEFAULT_FIELD.equalsIgnoreCase(tArr[0])) {
                throw new ElasticSql2DslException("[syntax error] Search fields name should one of [fields, default_field]");
            }
        }
    }

    private void setExtraMatchQueryParam(QueryStringQueryBuilder queryStringQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }

        if (extraParamMap.containsKey(Constants.MINIMUM_SHOULD_MATCH)) {
            String val = extraParamMap.get(Constants.MINIMUM_SHOULD_MATCH);
            queryStringQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey(Constants.ANALYZER)) {
            String val = extraParamMap.get(Constants.ANALYZER);
            queryStringQuery.analyzer(val);
        }

        if (extraParamMap.containsKey(Constants.QUOTE_ANALYZER)) {
            String val = extraParamMap.get(Constants.QUOTE_ANALYZER);
            queryStringQuery.quoteAnalyzer(val);
        }

        if (extraParamMap.containsKey(Constants.AUTO_GENERATE_PHRASE_QUERIES)) {
            String val = extraParamMap.get(Constants.AUTO_GENERATE_PHRASE_QUERIES);
            queryStringQuery.autoGeneratePhraseQueries(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(Constants.MAX_DETERMINIZED_STATES)) {
            String val = extraParamMap.get(Constants.MAX_DETERMINIZED_STATES);
            queryStringQuery.maxDeterminizedStates(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.ALLOW_LEADING_WILDCARD)) {
            String val = extraParamMap.get(Constants.ALLOW_LEADING_WILDCARD);
            queryStringQuery.allowLeadingWildcard(Boolean.parseBoolean(val));
        }


        if (extraParamMap.containsKey(Constants.ENABLE_POSITION_INCREMENTS)) {
            String val = extraParamMap.get(Constants.ENABLE_POSITION_INCREMENTS);
            queryStringQuery.enablePositionIncrements(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(Constants.FUZZY_PREFIX_LENGTH)) {
            String val = extraParamMap.get(Constants.FUZZY_PREFIX_LENGTH);
            queryStringQuery.fuzzyPrefixLength(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.FUZZY_MAX_EXPANSIONS)) {
            String val = extraParamMap.get(Constants.FUZZY_MAX_EXPANSIONS);
            queryStringQuery.fuzzyMaxExpansions(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.BOOST)) {
            String val = extraParamMap.get(Constants.BOOST);
            queryStringQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.FUZZY_REWRITE)) {
            String val = extraParamMap.get(Constants.FUZZY_REWRITE);
            queryStringQuery.fuzzyRewrite(val);
        }

        if (extraParamMap.containsKey(Constants.REWRITE)) {
            String val = extraParamMap.get(Constants.REWRITE);
            queryStringQuery.rewrite(val);
        }

        if (extraParamMap.containsKey(Constants.PHRASE_SLOP)) {
            String val = extraParamMap.get(Constants.PHRASE_SLOP);
            queryStringQuery.phraseSlop(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.ANALYZE_WILDCARD)) {
            String val = extraParamMap.get(Constants.ANALYZE_WILDCARD);
            queryStringQuery.analyzeWildcard(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(Constants.QUOTE_FIELD_SUFFIX)) {
            String val = extraParamMap.get(Constants.QUOTE_FIELD_SUFFIX);
            queryStringQuery.quoteFieldSuffix(val);
        }

        if (extraParamMap.containsKey(Constants.USE_DIS_MAX)) {
            String val = extraParamMap.get(Constants.USE_DIS_MAX);
            queryStringQuery.useDisMax(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(Constants.TIE_BREAKER)) {
            String val = extraParamMap.get(Constants.TIE_BREAKER);
            queryStringQuery.tieBreaker(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.TIME_ZONE)) {
            String val = extraParamMap.get(Constants.TIME_ZONE);
            queryStringQuery.timeZone(val);
        }

        if (extraParamMap.containsKey(Constants.ESCAPE)) {
            String val = extraParamMap.get(Constants.ESCAPE);
            queryStringQuery.escape(Boolean.parseBoolean(val));
        }
        if (extraParamMap.containsKey(Constants.DEFAULT_OPERATOR)) {
            String val = extraParamMap.get(Constants.DEFAULT_OPERATOR);

            if (Constants.AND.equalsIgnoreCase(val)) {
                queryStringQuery.defaultOperator(Operator.AND);
            }
            if (Constants.OR.equalsIgnoreCase(val)) {
                queryStringQuery.defaultOperator(Operator.OR);
            }
        }

        if (extraParamMap.containsKey(Constants.FUZZINESS)) {
            String val = extraParamMap.get(Constants.FUZZINESS).toLowerCase();

            switch (val){
                case "0":
                case "zero":{
                    queryStringQuery.fuzziness(Fuzziness.ZERO);
                    break;
                }
                case "1":
                case "one":{
                    queryStringQuery.fuzziness(Fuzziness.ONE);
                    break;
                }
                case "2":
                case "two":{
                    queryStringQuery.fuzziness(Fuzziness.TWO);
                    break;
                }
                default:{
                    queryStringQuery.fuzziness(Fuzziness.AUTO);
                }
            }
        }
    }
}
