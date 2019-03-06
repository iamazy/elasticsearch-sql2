package com.iamazy.springcloud.elasticsearch.dsl.sql.parser.sql;


import com.alibaba.druid.sql.ast.expr.SQLQueryExpr;
import com.iamazy.springcloud.elasticsearch.dsl.sql.druid.ElasticSqlSelectQueryBlock;
import com.iamazy.springcloud.elasticsearch.dsl.sql.listener.ParseActionListener;
import com.iamazy.springcloud.elasticsearch.dsl.sql.model.ElasticDslContext;
import org.elasticsearch.index.query.BoolQueryBuilder;

/**
 * @author iamazy
 */
public class QueryMatchConditionParser extends BoolExpressionParser implements QueryParser{

    public QueryMatchConditionParser(ParseActionListener parseActionListener) {
        super(parseActionListener);
    }

    @Override
    public void parse(ElasticDslContext dslContext) {

        if(dslContext.getSqlObject() instanceof SQLQueryExpr) {
            ElasticSqlSelectQueryBlock queryBlock = (ElasticSqlSelectQueryBlock) ((SQLQueryExpr) dslContext.getSqlObject()).getSubQuery().getQuery();
            if (queryBlock.getMatchQuery() != null) {
                String queryAs = dslContext.getParseResult().getQueryAs();
                BoolQueryBuilder matchQuery = parseBoolQueryExpr(queryBlock.getMatchQuery(), queryAs, dslContext.getSqlArgs());
                dslContext.getParseResult().setMatchCondition(matchQuery);
            }
        }
    }
}
