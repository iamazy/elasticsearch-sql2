package com.iamazy.elasticsearch.dsl.sql.model;

import com.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
@Getter
@AllArgsConstructor
public class ElasticSqlQueryField {
    private String nestedDocContextPath;
    private String simpleQueryFieldName;
    private String queryFieldFullName;
    private QueryFieldType queryFieldType;
}
