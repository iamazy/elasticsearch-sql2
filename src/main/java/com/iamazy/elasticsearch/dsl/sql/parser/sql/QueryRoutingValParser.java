package com.iamazy.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.google.common.collect.Lists;
import com.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import com.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import com.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import com.iamazy.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;
import org.apache.commons.collections4.CollectionUtils;


import java.util.List;

public class QueryRoutingValParser implements QueryParser {

    private ParseActionListener parseActionListener;

    public QueryRoutingValParser(ParseActionListener parseActionListener) {
        this.parseActionListener = parseActionListener;
    }

    @Override
    public void parse(ElasticDslContext dslContext) {

        if(dslContext.getSqlObject() instanceof SQLQueryExpr) {
            ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr) dslContext.getSqlObject()).getSubQuery().getQuery();
            if (queryBlock.getRouting() != null && CollectionUtils.isNotEmpty(queryBlock.getRouting().getRoutingValues())) {
                List<String> routingStringValues = Lists.newLinkedList();
                for (SQLExpr routingVal : queryBlock.getRouting().getRoutingValues()) {
                    if (routingVal instanceof SQLCharExpr) {
                        routingStringValues.add(((SQLCharExpr) routingVal).getText());
                    } else if (routingVal instanceof SQLVariantRefExpr) {
                        Object targetVal = ElasticSqlArgConverter.convertSqlArg(routingVal, dslContext.getSqlArgs());
                        routingStringValues.add(targetVal.toString());
                    } else {
                        throw new ElasticSql2DslException("[syntax error] Index routing val must be a string");
                    }
                }
                dslContext.getParseResult().setRoutingBy(routingStringValues);

                parseActionListener.onRoutingValuesParse(routingStringValues);
            }
        }
    }
}
