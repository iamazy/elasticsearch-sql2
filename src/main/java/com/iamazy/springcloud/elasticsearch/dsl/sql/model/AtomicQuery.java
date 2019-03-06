package com.iamazy.springcloud.elasticsearch.dsl.sql.model;


import lombok.Getter;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
@Getter
public class AtomicQuery {

    private QueryBuilder queryBuilder;
    private boolean isNestedQuery;
    private String nestedQueryPath;

    public AtomicQuery(QueryBuilder queryBuilder){
        this.queryBuilder=queryBuilder;
        this.isNestedQuery=false;
    }

    public AtomicQuery(QueryBuilder queryBuilder,String nestedQueryPath){
        this.queryBuilder=queryBuilder;
        this.isNestedQuery=true;
        this.nestedQueryPath=nestedQueryPath;
    }
}
