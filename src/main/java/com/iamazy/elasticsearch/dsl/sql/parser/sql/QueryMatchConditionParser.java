package com.iamazy.elasticsearch.dsl.sql.parser.sql;


import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.iamazy.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import com.iamazy.elasticsearch.dsl.sql.model.ElasticDslContext;
import org.elasticsearch.index.query.BoolQueryBuilder;

/**
 * @author iamazy
 */
public class QueryMatchConditionParser extends BoolExpressionParser implements QueryParser{

    @Override
    public void parse(ElasticDslContext dslContext) {

        if(dslContext.getSqlObject() instanceof SQLQueryExpr) {
            ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr) dslContext.getSqlObject()).getSubQuery().getQuery();
            if (queryBlock.getMatchQuery() != null) {
                String queryAs = dslContext.getParseResult().getQueryAs();
                BoolQueryBuilder matchQuery = parseBoolQueryExpr(queryBlock.getMatchQuery(), queryAs);
                dslContext.getParseResult().setMatchCondition(matchQuery);
            }
        }
    }
}
