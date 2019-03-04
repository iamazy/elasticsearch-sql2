package com.iamazy.springcloud.elasticsearch.dsl.sql.model;

import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.QueryFieldType;

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
public class ElasticSqlQueryFields {
    private ElasticSqlQueryFields(){}

    public static ElasticSqlQueryField newSqlSelectField(String fullPathFieldName){
        return new ElasticSqlQueryField(null,fullPathFieldName,fullPathFieldName, QueryFieldType.SqlSelectField);
    }

    public static ElasticSqlQueryField newMatchAllRootDocField(){
        return new ElasticSqlQueryField(null,"*","*",QueryFieldType.MatchAllField);
    }

    public static ElasticSqlQueryField newRootDocQueryField(String rootDocFieldName){
        return new ElasticSqlQueryField(null,rootDocFieldName,rootDocFieldName,QueryFieldType.RootDocField);
    }

    public static ElasticSqlQueryField newInnerDocQueryField(String innerDocFieldPrefix,String innerDocFieldName){
        String innerDocQueryFieldFullName=String.format("%s.%s",innerDocFieldPrefix,innerDocFieldName);
        return new ElasticSqlQueryField(null,innerDocFieldName,innerDocQueryFieldFullName,QueryFieldType.InnerDocField);
    }

    public static ElasticSqlQueryField newNestedDocQueryField(String nestedDocContextPath,String simpleQueryFieldName){
        String nestedDocQueryFieldFullName=String.format("%s.%s",nestedDocContextPath,simpleQueryFieldName);
        return new ElasticSqlQueryField(nestedDocContextPath,simpleQueryFieldName,nestedDocQueryFieldFullName,QueryFieldType.NestedDocField);
    }
}
































