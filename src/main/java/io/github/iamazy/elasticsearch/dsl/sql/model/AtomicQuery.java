package io.github.iamazy.elasticsearch.dsl.sql.model;


import lombok.Getter;
import lombok.Setter;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
@Getter
public class AtomicQuery {

    @Setter
    private Set<String> highlighter=new HashSet<>(0);

    private QueryBuilder queryBuilder;
    private boolean isNestedQuery;
    private ArrayList<String> nestedQueryPath;

    public AtomicQuery(QueryBuilder queryBuilder){
        this.queryBuilder=queryBuilder;
        this.isNestedQuery=false;
    }

    public AtomicQuery(QueryBuilder queryBuilder,ArrayList<String> nestedQueryPath){
        this.queryBuilder=queryBuilder;
        this.isNestedQuery=true;
        this.nestedQueryPath=nestedQueryPath;
    }
}
