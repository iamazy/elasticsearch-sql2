package org.iamazy.elasticsearch.dsl.sql.parser.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.google.common.collect.Lists;
import org.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import org.iamazy.elasticsearch.dsl.sql.exception.ElasticSql2DslException;
import org.iamazy.elasticsearch.dsl.sql.helper.ElasticSqlArgConverter;
import org.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;
import org.apache.commons.collections4.CollectionUtils;


import java.util.List;

public class QueryRoutingValParser implements QueryParser {


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
                        Object targetVal = ElasticSqlArgConverter.convertSqlArg(routingVal);
                        routingStringValues.add(targetVal.toString());
                    } else {
                        throw new ElasticSql2DslException("[syntax error] Index routing val must be a string");
                    }
                }
                dslContext.getParseResult().setRoutingBy(routingStringValues);
            }
        }
    }
}
