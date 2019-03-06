package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.google.common.collect.ImmutableList;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.AbstractFieldSpecificMethodQueryParser;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.springcloud.elasticsearch.dsl.sql.utils.Constants;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.*;

import java.util.List;
import java.util.Map;

/**
 * @author iamazy
 * @date 2019/2/20
 * @descrition
 **/
public class MatchPhrasePrefixQueryParser extends AbstractFieldSpecificMethodQueryParser {

    MatchPhrasePrefixQueryParser(ParseActionListener parseActionListener){
        super(parseActionListener);
    }

    private static final List<String> MATCH_PHRASE_PREFIX_METHOD = ImmutableList.of("match_phrase_prefix", "match_phrase_prefix_query", "matchPhrasePrefixQuery");

    @Override
    protected QueryBuilder buildQuery(MethodInvocation invocation, String fieldName, Map<String, String> extraParams) {
        String text=invocation.getParameterAsString(1);
        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder= QueryBuilders.matchPhrasePrefixQuery(fieldName,text);
        setExtraMatchQueryParam(matchPhrasePrefixQueryBuilder,extraParams);
        return matchPhrasePrefixQueryBuilder;
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
        return MATCH_PHRASE_PREFIX_METHOD;
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

    private void setExtraMatchQueryParam(MatchPhrasePrefixQueryBuilder matchQuery, Map<String, String> extraParamMap) {
        if (MapUtils.isEmpty(extraParamMap)) {
            return;
        }

        if (extraParamMap.containsKey(Constants.ANALYZER)) {
            String val = extraParamMap.get(Constants.ANALYZER);
            matchQuery.analyzer(val);
        }

        if (extraParamMap.containsKey(Constants.BOOST)) {
            String val = extraParamMap.get(Constants.BOOST);
            matchQuery.boost(Float.valueOf(val));
        }

        if (extraParamMap.containsKey(Constants.MAX_EXPANSIONS)) {
            String val = extraParamMap.get(Constants.MAX_EXPANSIONS);
            matchQuery.maxExpansions(Integer.valueOf(val));
        }
    }
}
