package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.body;

import com.google.common.collect.ImmutableList;
import com.iamazy.springcloud.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.AtomicQuery;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodInvocation;
import com.iamazy.springcloud.elasticsearch.dsl.sql.parser.query.method.MethodQueryParser;

import java.util.List;

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
 * @date 2019/2/23
 * @descrition
 **/
public class HighlightAtomicQueryParser implements MethodQueryParser {

    private static List<String> HIGHLIGHT_METHOD = ImmutableList.of("highlight");

    @Override
    public AtomicQuery parseAtomMethodQuery(MethodInvocation invocation) throws ElasticSql2DslException {
        return null;
    }

    @Override
    public List<String> defineMethodNames() {
        return HIGHLIGHT_METHOD;
    }

    @Override
    public boolean isMatchMethodInvocation(MethodInvocation invocation) {
        return false;
    }

    @Override
    public void checkMethodInvocation(MethodInvocation invocation) throws ElasticSql2DslException {
        if (invocation.getParameterCount() != 2 || invocation.getParameterCount() != 4) {
            throw new ElasticSql2DslException(
                    String.format("[syntax error] There's no %s args method named [%s].",
                            invocation.getParameterCount(), invocation.getMethodName()));
        }
    }
}
