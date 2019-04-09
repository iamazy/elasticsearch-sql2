package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.google.common.collect.ImmutableList;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.ParameterizedMethodQueryParser;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import io.github.iamazy.elasticsearch.dsl.cons.ElasticConstants;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;

import java.util.List;
import java.util.Map;

public class QueryStringQueryParser extends ParameterizedMethodQueryParser {

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
        boolean highlighter=false;
        if(text.startsWith("h#")){
            text=text.substring(2);
            highlighter=true;
        }
        QueryStringQueryBuilder queryStringQuery = QueryBuilders.queryStringQuery(text);

        String queryFields ;
        if (invocation.getParameterCount() == 3) {
            queryFields = invocation.getParameterAsString(1);
            if (StringUtils.isNotBlank(queryFields)) {
                String[] tArr = queryFields.split(COLON);
                if (ElasticConstants.FIELDS.equalsIgnoreCase(tArr[0])) {
                    for (String fieldItem : tArr[1].split(COMMA)) {
                        queryStringQuery.field(fieldItem);
                    }
                }
                if (ElasticConstants.DEFAULT_FIELD.equalsIgnoreCase(tArr[0])) {
                    queryStringQuery.defaultField(tArr[1]);
                }
            }
        }

        setExtraMatchQueryParam(queryStringQuery, extraParamMap);
        if(highlighter){
            AtomicQuery atomicQuery=new AtomicQuery(queryStringQuery);
            atomicQuery.setHighlighter("*");
            return atomicQuery;
        }else {
            return new AtomicQuery(queryStringQuery);
        }
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

            if (Boolean.FALSE == ElasticConstants.FIELDS.equalsIgnoreCase(tArr[0]) && Boolean.FALSE == ElasticConstants.DEFAULT_FIELD.equalsIgnoreCase(tArr[0])) {
                throw new ElasticSql2DslException("[syntax error] Search fields name should one of [fields, default_field]");
            }
        }
    }

    private void setExtraMatchQueryParam(QueryStringQueryBuilder queryStringQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }

        if (extraParamMap.containsKey(ElasticConstants.MINIMUM_SHOULD_MATCH)) {
            String val = extraParamMap.get(ElasticConstants.MINIMUM_SHOULD_MATCH);
            queryStringQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.ANALYZER)) {
            String val = extraParamMap.get(ElasticConstants.ANALYZER);
            queryStringQuery.analyzer(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.QUOTE_ANALYZER)) {
            String val = extraParamMap.get(ElasticConstants.QUOTE_ANALYZER);
            queryStringQuery.quoteAnalyzer(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.AUTO_GENERATE_PHRASE_QUERIES)) {
            String val = extraParamMap.get(ElasticConstants.AUTO_GENERATE_PHRASE_QUERIES);
            queryStringQuery.autoGeneratePhraseQueries(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.MAX_DETERMINIZED_STATES)) {
            String val = extraParamMap.get(ElasticConstants.MAX_DETERMINIZED_STATES);
            queryStringQuery.maxDeterminizedStates(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.ALLOW_LEADING_WILDCARD)) {
            String val = extraParamMap.get(ElasticConstants.ALLOW_LEADING_WILDCARD);
            queryStringQuery.allowLeadingWildcard(Boolean.parseBoolean(val));
        }


        if (extraParamMap.containsKey(ElasticConstants.ENABLE_POSITION_INCREMENTS)) {
            String val = extraParamMap.get(ElasticConstants.ENABLE_POSITION_INCREMENTS);
            queryStringQuery.enablePositionIncrements(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.FUZZY_PREFIX_LENGTH)) {
            String val = extraParamMap.get(ElasticConstants.FUZZY_PREFIX_LENGTH);
            queryStringQuery.fuzzyPrefixLength(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.FUZZY_MAX_EXPANSIONS)) {
            String val = extraParamMap.get(ElasticConstants.FUZZY_MAX_EXPANSIONS);
            queryStringQuery.fuzzyMaxExpansions(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.BOOST)) {
            String val = extraParamMap.get(ElasticConstants.BOOST);
            queryStringQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.FUZZY_REWRITE)) {
            String val = extraParamMap.get(ElasticConstants.FUZZY_REWRITE);
            queryStringQuery.fuzzyRewrite(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.REWRITE)) {
            String val = extraParamMap.get(ElasticConstants.REWRITE);
            queryStringQuery.rewrite(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.PHRASE_SLOP)) {
            String val = extraParamMap.get(ElasticConstants.PHRASE_SLOP);
            queryStringQuery.phraseSlop(Integer.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.ANALYZE_WILDCARD)) {
            String val = extraParamMap.get(ElasticConstants.ANALYZE_WILDCARD);
            queryStringQuery.analyzeWildcard(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.QUOTE_FIELD_SUFFIX)) {
            String val = extraParamMap.get(ElasticConstants.QUOTE_FIELD_SUFFIX);
            queryStringQuery.quoteFieldSuffix(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.USE_DIS_MAX)) {
            String val = extraParamMap.get(ElasticConstants.USE_DIS_MAX);
            queryStringQuery.useDisMax(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.TIE_BREAKER)) {
            String val = extraParamMap.get(ElasticConstants.TIE_BREAKER);
            queryStringQuery.tieBreaker(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.TIME_ZONE)) {
            String val = extraParamMap.get(ElasticConstants.TIME_ZONE);
            queryStringQuery.timeZone(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.ESCAPE)) {
            String val = extraParamMap.get(ElasticConstants.ESCAPE);
            queryStringQuery.escape(Boolean.parseBoolean(val));
        }
        if (extraParamMap.containsKey(ElasticConstants.DEFAULT_OPERATOR)) {
            String val = extraParamMap.get(ElasticConstants.DEFAULT_OPERATOR);

            if (ElasticConstants.AND.equalsIgnoreCase(val)) {
                queryStringQuery.defaultOperator(Operator.AND);
            }
            if (ElasticConstants.OR.equalsIgnoreCase(val)) {
                queryStringQuery.defaultOperator(Operator.OR);
            }
        }

        if (extraParamMap.containsKey(ElasticConstants.FUZZINESS)) {
            String val = extraParamMap.get(ElasticConstants.FUZZINESS).toLowerCase();

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
