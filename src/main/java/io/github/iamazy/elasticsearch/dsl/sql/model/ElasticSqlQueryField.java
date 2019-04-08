package io.github.iamazy.elasticsearch.dsl.sql.model;

import io.github.iamazy.elasticsearch.dsl.sql.enums.QueryFieldType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
@Getter
@AllArgsConstructor
public class ElasticSqlQueryField {
    /**
     * 最多支持两层nested类型，再多就要考虑数据结构是否合理了
     */
    private ArrayList<String> nestedDocContextPath;
    private String simpleQueryFieldName;
    @Setter
    private String queryFieldFullName;
    private QueryFieldType queryFieldType;

    public ElasticSqlQueryField(ArrayList<String> nestedDocContextPath,QueryFieldType queryFieldType){
        this.nestedDocContextPath=nestedDocContextPath;
        this.queryFieldType=queryFieldType;
    }
}
