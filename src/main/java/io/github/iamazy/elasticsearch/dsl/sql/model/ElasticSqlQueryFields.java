package io.github.iamazy.elasticsearch.dsl.sql.model;

import io.github.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;

import java.util.ArrayList;

/**
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

    public static ElasticSqlQueryField newNestedDocQueryField(ArrayList<String> nestedDocContextPath, String simpleQueryFieldName){
        return new ElasticSqlQueryField(nestedDocContextPath,simpleQueryFieldName,simpleQueryFieldName,QueryFieldType.NestedDocField);
    }
}
































