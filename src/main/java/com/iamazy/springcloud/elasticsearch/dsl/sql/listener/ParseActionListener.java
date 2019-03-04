package com.iamazy.springcloud.elasticsearch.dsl.sql.listener;

import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticSqlQueryField;

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
 * @date 2019/2/19
 * @descrition
 **/
public interface ParseActionListener {
    void onSelectFieldParse(ElasticSqlQueryField field);
    void onAtomicExactQueryConditionParse(ElasticSqlQueryField paramName, Object[] params, SqlConditionOperator operator);
    void onAtomicMethodQueryConditionParse(ElasticSqlQueryField paramName, Object[] params);
    void onRoutingValuesParse(List<String> routingValues);
    void onLimitSizeParse(int from, int size);
}

































