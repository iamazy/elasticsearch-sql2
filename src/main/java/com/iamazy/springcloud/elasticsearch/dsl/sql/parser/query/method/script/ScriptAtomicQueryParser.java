package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.script;

import com.google.common.collect.ImmutableList;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.ParameterizedMethodQueryParser;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;

import java.util.List;
import java.util.Map;

public class ScriptAtomicQueryParser extends ParameterizedMethodQueryParser {

    private static List<String> SCRIPT_METHOD = ImmutableList.of("script_query", "scriptQuery");

    @Override
    public List<String> defineMethodNames() {
        return SCRIPT_METHOD;
    }

    @Override
    protected String defineExtraParamString(MethodInvocation invocation) {
        int extraParamIdx = 1;

        return (invocation.getParameterCount() == extraParamIdx + 1)
                ? invocation.getParameterAsString(extraParamIdx) : StringUtils.EMPTY;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 1 && invocation.getParameterCount() != 2) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }

        String script = invocation.getParameterAsString(0);
        if (StringUtils.isEmpty(script)) {
            throw new ElasticSql2DslException("[syntax error] Script can not be blank!");
        }
    }

    @Override
    protected AtomicQuery parseMethodQueryWithExtraParams(MethodInvocation invocation, Map<String, String> extraParamMap) throws ElasticSql2DslException {
        String script = invocation.getParameterAsString(0);

        if (MapUtils.isNotEmpty(extraParamMap)) {
            Map<String, Object> scriptParamMap = generateRawTypeParameterMap(invocation);
            return new AtomicQuery(QueryBuilders.scriptQuery(
                    new Script(Script.DEFAULT_SCRIPT_TYPE, Script.DEFAULT_SCRIPT_LANG, script, scriptParamMap))
            );
        }
        return new AtomicQuery(QueryBuilders.scriptQuery(new Script(script)));
    }


}
