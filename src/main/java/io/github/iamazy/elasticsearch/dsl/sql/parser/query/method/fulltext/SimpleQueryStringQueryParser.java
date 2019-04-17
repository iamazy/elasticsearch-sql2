package io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.github.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import io.github.iamazy.elasticsearch.dsl.sql.parser.query.method.ParameterizedMethodQueryParser;
import io.github.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import io.github.iamazy.elasticsearch.dsl.cons.ElasticConstants;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SimpleQueryStringBuilder;
import org.elasticsearch.index.query.SimpleQueryStringFlag;

import java.util.List;
import java.util.Map;

import static io.github.iamazy.elasticsearch.dsl.cons.CoreConstants.COLON;
import static io.github.iamazy.elasticsearch.dsl.cons.CoreConstants.COMMA;

public class SimpleQueryStringQueryParser extends ParameterizedMethodQueryParser {

    private static List<String> SIMPLE_QUERY_STRING_METHOD = ImmutableList.of("simpleQueryString", "simple_query_string");

    @Override
    public List<String> defineMethodNames() {
        return SIMPLE_QUERY_STRING_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        return isExtraParamsString(invocation.getLastParameterAsString())
                ? invocation.getLastParameterAsString() : StringUtils.EMPTY;
    }

    @Override
    protected AtomicQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        String text = invocation.getParameterAsString(0);
        SimpleQueryStringBuilder simpleQueryString = QueryBuilders.simpleQueryStringQuery(text);

        String queryFields ;
        if (invocation.getParameterCount() == 3) {
            queryFields = invocation.getParameterAsString(1);

            if (StringUtils.isNotBlank(queryFields)) {
                String[] tArr = queryFields.split(COLON);
                if (ElasticConstants.FIELDS.equalsIgnoreCase(tArr[0])) {
                    for (String fieldItem : tArr[1].split(COMMA)) {
                        simpleQueryString.field(fieldItem);
                    }
                }
            }
        }

        if (MapUtils.isNotEmpty(extraParamMap)) {
            setExtraMatchQueryParam(simpleQueryString, extraParamMap);
        }

        return new AtomicQuery(simpleQueryString);
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

            if (Boolean.FALSE == ElasticConstants.FIELDS.equalsIgnoreCase(tArr[0])) {
                throw new ElasticSql2DslException("[syntax error] Search fields name should one of [fields]");
            }
        }
    }

    private void setExtraMatchQueryParam(SimpleQueryStringBuilder simpleStringQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }

        if (extraParamMap.containsKey(ElasticConstants.MINIMUM_SHOULD_MATCH)) {
            String val = extraParamMap.get(ElasticConstants.MINIMUM_SHOULD_MATCH);
            simpleStringQuery.minimumShouldMatch(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.ANALYZER)) {
            String val = extraParamMap.get(ElasticConstants.ANALYZER);
            simpleStringQuery.analyzer(val);
        }

        if (extraParamMap.containsKey(ElasticConstants.BOOST)) {
            String val = extraParamMap.get(ElasticConstants.BOOST);
            simpleStringQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.ANALYZE_WILDCARD)) {
            String val = extraParamMap.get(ElasticConstants.ANALYZE_WILDCARD);
            simpleStringQuery.analyzeWildcard(Boolean.parseBoolean(val));
        }

        if (extraParamMap.containsKey(ElasticConstants.FLAGS)) {
            String[] flags = extraParamMap.get(ElasticConstants.FLAGS).split("\\|");
            List<SimpleQueryStringFlag> flagList = Lists.newLinkedList();
            for (String flag : flags) {
                flagList.add(SimpleQueryStringFlag.valueOf(flag.toUpperCase()));
            }
            simpleStringQuery.flags(flagList.toArray(new SimpleQueryStringFlag[0]));
        }


        if (extraParamMap.containsKey(ElasticConstants.DEFAULT_OPERATOR)) {
            String val = extraParamMap.get(ElasticConstants.DEFAULT_OPERATOR);

            if (ElasticConstants.AND.equalsIgnoreCase(val)) {
                simpleStringQuery.defaultOperator(Operator.AND);
            }
            if (ElasticConstants.OR.equalsIgnoreCase(val)) {
                simpleStringQuery.defaultOperator(Operator.OR);
            }
        }
    }
}
