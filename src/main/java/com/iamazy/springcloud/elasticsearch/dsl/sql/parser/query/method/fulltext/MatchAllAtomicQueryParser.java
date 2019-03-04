package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.fulltext;

import com.google.common.collect.ImmutableList;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.ParameterizedMethodQueryParser;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.List;
import java.util.Map;

/**
 * Copyright 2018-2019 iamazy Logic Ltd
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @author iamazy
 * @date 2019/3/4
 * @descrition
 **/
public class MatchAllAtomicQueryParser extends ParameterizedMethodQueryParser {

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
