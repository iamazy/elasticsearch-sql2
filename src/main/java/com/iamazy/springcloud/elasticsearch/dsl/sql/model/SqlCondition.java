package com.iamazy.springcloud.elasticsearch.dsl.sql.model;

import com.google.common.collect.Lists;
import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlBoolOperator;
import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlConditionType;
import lombok.Getter;

import java.util.List;

/**
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
