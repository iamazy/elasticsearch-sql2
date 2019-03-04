package com.iamazy.springcloud.elasticsearch.dsl.sql.model;

import com.google.common.collect.Lists;
import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlBoolOperator;
import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlConditionType;
import lombok.Getter;

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

@Getter
public class SqlCondition {
    private SqlConditionType conditionType;
    private SqlBoolOperator operator;
    private List<AtomicQuery> queryList;

    public SqlCondition(AtomicQuery query){
        this(query,SqlConditionType.Atom);
    }

    public SqlCondition(List<AtomicQuery> queryList,SqlBoolOperator operator){
        this.queryList=queryList;
        this.conditionType=SqlConditionType.Combine;
        this.operator=operator;
    }

    public SqlCondition(AtomicQuery query,SqlConditionType conditionType){
        this.queryList= Lists.newArrayList(query);
        this.conditionType=conditionType;
    }
}
