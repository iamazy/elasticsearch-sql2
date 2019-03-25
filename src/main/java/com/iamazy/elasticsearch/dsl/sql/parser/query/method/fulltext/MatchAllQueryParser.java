package com.iamazy.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.google.common.collect.ImmutableList;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.ParameterizedMethodQueryParser;
import com.iamazy.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;
import java.util.Map;

/**
 * @author iamazy
 * @date 2019/3/4
 * @descrition
 **/
public class MatchAllQueryParser extends ParameterizedMethodQueryParser {

    private static final List<String> MATCH_ALL_METHOD = ImmutableList.of("matchAll", "match_all", "matchAllQuery","match_all_query");


    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        return isExtraParamsString(invocation.getLastParameterAsString())
                ? invocation.getLastParameterAsString(): StringUtils.EMPTY;
    }

    @Override
    protected AtomicQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        return new AtomicQuery(QueryBuilders.matchAllQuery());
    }

    @Override
    public List<String> defineMethodNames() {
        return MATCH_ALL_METHOD;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        int paramCount=invocation.getParameterCount();
        if(paramCount!=0){
            throw new ElasticSql2DslException(
                    String.format("[syntax error] The method named [%s] params must be empty", invocation.getMethodName()));
        }
    }
}
