package com.iamazy.springcloud.elasticsearch.dsl.sql.listener;

import com.iamazy.springcloud.elasticsearch.dsl.sql.enums.SqlConditionOperator;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticSqlQueryField;

import java.util.List;

/**
 * @author iamazy
 * @date 2019/2/19
 * @descrition
 **/
public class ParseActionListenerAdapter implements ParseActionListener {
    @Override
    public void onSelectFieldParse(ElasticSqlQueryField field) {

    }

    @Override
    public void onAtomicExactQueryConditionParse(ElasticSqlQueryField paramName, Object[] params, SqlConditionOperator operator) {

    }

    @Override
    public void onAtomicMethodQueryConditionParse(ElasticSqlQueryField paramName, Object[] params) {

    }

    @Override
    public void onRoutingValuesParse(List<String> routingValues) {

    }

    @Override
    public void onLimitSizeParse(int from, int size) {

    }
}
