package com.iamazy.springcloud.elasticsearch.dsl.sql.listener;

import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticSqlQueryField;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
public interface ParseActionListener {
    void onSelectFieldParse(ElasticSqlQueryField field);
    void onAtomicExactQueryConditionParse(ElasticSqlQueryField paramName, Object[] params, SqlConditionOperator operator);
    void onAtomicMethodQueryConditionParse(ElasticSqlQueryField paramName,Object[] params);
    void onRoutingValuesParse(List<String> routingValues);
    void onLimitSizeParse(int from,int size);
}

































